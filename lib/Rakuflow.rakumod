#!/usr/bin/env raku

unit module Rakuflow;

use JSON::Fast;
use Gcrypt::Simple :MD5;

our $*process-name is export;

class UUID {
    has $.bytes;
    has $.version;
method new(:$version = 4) {
        if $version == 4 {
            my @bytes = (0..255).roll(16);

            #variant
            @bytes[8] +|= 0b10000000;
            @bytes[8] +&= 0b10111111;

            #version
            @bytes[6] +|= 0b01000000;
            @bytes[6] +&= 0b01001111;

            self.bless(:bytes(buf8.new(@bytes)), :$version);
        }
        else {
            die "UUID version $version not supported.";
        }
    }

    method Str {
        (:256[$.bytes.values].fmt("%32.32x")
            ~~ /(........)(....)(....)(....)(............)/)
            .join("-");
    }

    method Blob {
        $.bytes;
    }
}

class Path is Str is export {}

Str.^add_method( 'Path', method () returns Path:D {#{{{
  return Path(self);
} );#}}}

## PBS run
grammar JOB_INFO {
  token TOP { 'Job Id: '<job_id>\n<key-value>+\n* }
  token job_id { \N+ }
  token key-value { \N+\n }
}

class JOB_INFO-actions {
  method TOP ($/) {
    make $<key-value>.map({
      my $key-value = $_.trim.split(' = ');
      $key-value[0] => $key-value[1]
    }).Hash
  }
}

multi run("bash", $script, $cwd, $job-name, $resource) {
  my $log-out = "$cwd/qsub.out.log";
  my $log-err = "$cwd/qsub.err.log";
  my $proc = run "bash", $script, :cwd($cwd), :out, :err;
  spurt $log-out, $proc.out.slurp;
  spurt $log-err, $proc.err.slurp;
  $*ERR.print: $log-err.IO.slurp;
  print $log-out.IO.slurp;
  $proc.exitcode;
}

multi run("qsub", $script, $cwd, $job-name, $resource) {
  my $log-out = "$cwd/qsub.out.log";
  my $log-err = "$cwd/qsub.err.log";
  my $resource-cmd = "";
  if $resource ~~ Hash {
    my $node-n = $resource<node> // 1;
    $resource-cmd = $resource.kv.map( -> $k, $v {
        when $k eq "cpu" { qq[nodes\={$node-n}\:ppn\={$v}] }
        when $k eq "memory" { qq[mem\={$v}]}
        when $k eq "time" { qq[walltime\={$v}] }
    }).join(",");
    $resource-cmd = "-l $resource-cmd";
  } elsif $*resource ~~ Str {
    $resource-cmd = "-l $*resource";
  }
  my $qsub-out = qqx[qsub $script $resource-cmd -N $job-name -d $cwd -o $log-out -e $log-err].trim;
  say "[PBS] Start job: $qsub-out for $job-name";
  my $job-id = ($qsub-out ~~ m/(\d+)\./)[0].Str;

  my $p = monitor-job($job-id);
  my $o = monitor-file($log-out, $p);
  my $e = monitor-file($log-err, $p);
  my $return-value;
  react {
    whenever $p -> $r {
      $return-value = $r;
    }
    whenever $o -> $l {
      say $l.chomp;
    }
    whenever $e -> $l {
      note $l.chomp;
    }
  }
  $return-value;
}

sub monitor-job($job-id) {
  my $prom = Promise.start({
    my $demo-qstat-out;
    my $status;
    my $job-info;
    loop {
      sleep 5;
      $demo-qstat-out = qqx[qstat -f1 $job-id];
      $job-info = JOB_INFO.parse($demo-qstat-out, :actions(JOB_INFO-actions)).made;
      if so $job-info<job_state> eq ['C', 'E'].any {
        $status = $job-info<exit_status>;
        last;
      } else {
        sleep 1;
      }
    }
    $status;
  });
  return $prom;
}

sub monitor-file($file, $job-prom) {
  my $fh;

  my $supply = supply {
    loop {
      if not $fh {
        $fh = $file.IO.open(:r) if ($file.IO.e);
      } else {
        while not $fh.eof {
          if $fh.get -> $l {
            emit $l;
          }
        }
        if $job-prom.status ~~ "Kept" {
          sleep 2;
          while not $fh.eof {
            if $fh.get -> $l {
              emit $l;
            }
          }
          sleep 1;
          done();
        };
        sleep 10;
      }
    }
  }
  return $supply
}

class Process is export {#{{{
  has Hash $.input;
  has Hash $.output;
  has Str $.script;
  has Str $.workdir = Any;
  has Str $.code = "";
  has Str $.exportDir = [];
  has Hash $.config;

  submethod TWEAK {
    unless $!workdir {
      $!workdir = $!config<workdir> // $*CWD;
    }
  }

  method run(*%input) {
    my $cwd = prepare-workdir($!workdir);
    my $code = process_template($!code, %input, $!config);
    Promise.start({
      run-code($code, $!workdir);
      $!output.kv.map( -> $k, $v {
        # TODO define File type
        if $v ~~ Str {
          "$cwd/$k".IO
        } elsif $v ~~ IO {
          $k.IO
        } else {
          $k
        }
      });
    });
  }

  sub process_template($template is copy, %input, %config) {
    $template ~~ s:g/\{\{(\w+)\}\}/$(extract_param($0, %input, %config))/;
    return $template;
  }

  sub extract_param($query, %param, %config) {
    %param{$query} // %config{$query}
  }
}#}}}

sub get-new_uuid() is export {
  return UUID.new.Str.substr(0, 5);
}

sub prepare-workdir($workdir) is export {
  my $uuid = get-new_uuid();
  my $new-dir = qq[$workdir/workdir/$uuid/];
  mkdir $new-dir;
  return $new-dir;
}

sub run-code($code, $cwd, $process-name, $proc, $resource) is export {
  my $script = "$cwd/script.txt";
  my $exitcode;
  my $job-name = 'rf-' ~ $process-name;
  spurt $script, $code;
  if $proc eq "bash" {
    $exitcode = run "bash", $script, $cwd, $job-name, $resource;
  } elsif $proc eq "PBS" {
    $exitcode = run "qsub", $script, $cwd, $job-name, $resource;
  }
  { exitcode => $exitcode }
}

sub export-file($from, $export-to) is export {
  mkdir $export-to unless $export-to.IO.d;
  my $to = $export-to ~ "/" ~ $from.IO.basename;
  qqx[ln -s $from $to];
}

sub encode-job-info(%proc-info is copy) {
  if %proc-info<output> ~~ Hash {
    %proc-info<output> = %proc-info<output>.map({
      say $_;
      if $_ ~~ IO {
        $_ ~ ".IO" ~ {$_.changed.DataTime}
      } else {
        $_
      }
    })
  }
  MD5(%proc-info.perl).hex
}

sub process(:$workdir, :$code, :$output, :$export-to = Any, :$proc-bin is copy = 'bash', :$process-name = $*process-name, :$resource = $*resource) is export {
  my %proc-info = workdir => $workdir, code => $code;
  my $job_info = encode-job-info(%proc-info);
  my $cwd;
  my $hist := $*hist;
  $proc-bin = $*proc-bin if $*proc-bin;

  my $prom = Promise.start({

    if $hist{$job_info} {
      $cwd = $hist{$job_info};
      note "[$process-name] Resume proc: $job_info with CWD $cwd";
    } else {
      $cwd = prepare-workdir($workdir);
      note "[$process-name] Start proc: $job_info with CWD $cwd";
      my $proc;
      $proc = run-code($code, $cwd, $process-name, $proc-bin, $resource);

      unless $proc<exitcode> == 0 {
        note "Error happend when run: $process-name";
        note "Job: $job_info";
        note "============== Code =============";
        note $code;
        note "============== CWD ==============";
        note $cwd;
        note "============== More =============";
        note $resource;
      }

      $hist{$job_info} = $cwd;
      "$workdir/.history".IO.spurt(to-json(%$hist));
    }

    ## export the output
    if $export-to {
      my @output;
      if $output ~~ Hash {
        @output = %$output.values;
      } else {
        @output = @$output;
      }
      for @output -> $from {
        if $from ~~ Path {
          export-file("$cwd/$from", $export-to);
        } elsif $from ~~ IO {
          export-file("$from", $export-to);
        }
      }
    }

    ## process output
    # TODO: Output the stdout
    if $output ~~ Hash {
      %$output.map({
        $_.key => $_.value ~~ Path ?? "$cwd/{$_.value}".IO !! $_.value
      }).Hash;
    } else {
      @$output.map({
        $_ ~~ Path ?? "$cwd/$_".IO !! $_
      }).Array;
    }
  });

  return $prom;
}



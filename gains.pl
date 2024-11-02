#!/usr/bin/perl -w
use strict;
use FileHandle;
use POSIX;

my $volts_file = $ARGV[0];
my $gains_file = $ARGV[1];

die "Usage: ".$0." voltage_file gains_file"
  if((not defined $volts_file)||(not defined $gains_file));

my @parts;

my $file_handle = new FileHandle($volts_file,"r");
die "Could not open ".$volts_file if(not defined $file_handle);

my $line;

while(defined($line = $file_handle->getline()))
  {
    $line =~ s/\s+$//;
    $line =~ s/^\s+//;
    next if(not $line);

    push @parts,[split(/\s+/,$line)];
  }
undef $file_handle;

print STDERR "Found: ",scalar(@parts)," HV entries\n";

$file_handle = new FileHandle($gains_file,"r");
die "Could not open ".$gains_file if(not defined $file_handle);

my @changes;
my @nochanges;
my $channel=0;
while(defined($line = $file_handle->getline()))
  {
    $line =~ s/\s+$//;
    $line =~ s/^\s//;
    next if(not $line);

#    print STDERR $channel,"\t",$parts[$channel][7]*(pow($line,-1/8)-1),"\n";
    push @changes,[$channel+1, $parts[$channel][7]*(pow($line,1/8)-1)];
    push @nochanges,$channel+1 if ($line == 1.0);
    $parts[$channel][7] = sprintf("%.1f",$parts[$channel][7]*pow($line,1/8));

    $channel++;
  }
undef $file_handle;

my $top10=10;
foreach(sort { abs($b->[1]) <=> abs($a->[1]) } @changes)
  {
    if($top10)
      {
	$top10--;
	printf STDERR ("Channel: %3d  Change: %.1f\n",$_->[0],$_->[1]);
      }
  }
print STDERR "No changes: ",join(",",@nochanges),"\n";

foreach(@parts)
  {
    print join("\t", @{$_}),"\n";
  }


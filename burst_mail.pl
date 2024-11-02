use POSIX;

sub hms
  {
    my $angle=shift;
    my $h=int(abs($angle)/15);
    my $m=int(fmod(abs($angle)*4,60));
    my $s=int(fmod(abs($angle)*240,60));
    return sprintf("%7.6d",($h*10000+$m*100+$s)*(($angle>0)?1:-1));
  }

sub dms
  {
    my $angle=shift;
    my $d=int(abs($angle));
    my $m=int(fmod(abs($angle)*60,60));
    my $s=int(fmod(abs($angle)*3600,60));
    return sprintf("%7.6d",($d*10000+$m*100+$s)*(($angle>0)?1:-1));
  }

foreach(<ARGV>)
{
	s/^\s+//;
	($ra,$dec,@rest)=split /\s+/;
	$ra *= 15;
	print hms($ra),"  ",dms($dec),"\n";
}

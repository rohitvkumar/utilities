#!/usr/local/bin/perl -w

use warnings;
use strict;

# quick and dirty.

my $yes = 1;

my $file = shift || 'images2';

open my $fh, "<", $file;
my @images = <$fh>;
close $fh;

$| = 1;

foreach my $image ( @images ) {

   # image should look like : 
   # docker.tivo.com:443/catalog-server:latest

   chomp $image;

   my $pull = "docker pull $image";

   print "$pull\n";
   if ( $yes ) { system ($pull); }

   my $remote = $image;

   # XXX
   # /etc/hosts needs to have www.docker.tivo.com pointing at the remote site. 
   # this whole bit relies on a hack that our docker.tivo.com SSL cert is also
   # valid for www.docker.tivo.com. so we pull from docker.tivo.com and push
   # to "www.docker.tivo.com".
   # XXX

   $remote =~ s/docker\.tivo\.com/www\.docker\.tivo\.com/;

   my $tag  = "docker tag $image $remote";

   print "$tag\n";
   if ( $yes ) { system ($tag); }

   my $push = "time docker push $remote";

   print "$push\n"; 
   if ( $yes ) { system ($push); }

   my $clean  = "docker rmi -f $remote";
   my $clean2 = "docker rmi -f $image";

   print "$clean\n";
   print "$clean2\n";

   if ( $yes ) { 
#      system ($clean);
#      system ($clean2);
   }

}

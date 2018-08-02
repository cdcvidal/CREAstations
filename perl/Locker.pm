#!/usr/bin/perl

package Locker;

use CreaCommons;
use strict;

sub new {
	my $class = shift;
	my $lockFilename = shift;
	my $self = bless({
		lockFilename => $lockFilename,
		lockFileCreated => 0
	}, $class);
	
	if (!defined($self->{lockFilename}) || $self->{lockFilename} =~ /^\s*$/) {
		$CreaCommons::logger->logdie("No lock filename provided in Locker initialisation");
	}
	if (-e $self->{lockFilename}) {
		$CreaCommons::logger->logdie("Application already running: exiting...");
	} else {
		open(my $file, ">>", "$self->{lockFilename}") or $CreaCommons::logger->logdie("Failed to create lockfile '$self->{lockFilename}': $!");
		print $file "lock\n";
		close $file;
		$self->{lockFileCreated} = 1;
	}
	return $self;
}

sub DESTROY {
	my $self = shift;
	if ($self->{lockFileCreated} == 1) {
		$CreaCommons::logger->info("Deleting lock file.");
		unlink $self->{lockFilename} or $CreaCommons::logger->logwarn("Failed to delete lock file '$self->{lockFilename}': $!");
	}
}

sub lockFilename { $_[0]->{lockFilename}=$_[1] if defined $_[1]; $_[0]->{lockFilename} }
	
1;
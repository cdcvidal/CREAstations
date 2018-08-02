#!/usr/bin/perl

use File::Copy;
use File::Basename; 
use Config::IniFiles;
use DBI;
use Locker;
use CreaCommons;
use POSIX qw(strftime);
use Time::Piece;
use Time::Seconds;
use strict;
use warnings; # TODO remove?

# Configuration
my %conf;

# Handle to sqlite db
my $dbh;

my $configFilePath = "conf.ini";

sub connectToSqliteDb() {
	$dbh = DBI->connect("dbi:SQLite:dbname=".$conf{"dbName"}, "", "", {
		PrintError	=> 0,
		RaiseError	=> 1,
		AutoCommit	=> 1}) 
		or $CreaCommons::logger->logdie("Failed to open database '".$conf{"dbName"}."': $DBI::errstr");
}

sub checkStation($$$$$) {
	my $stationName = shift;
	my $lastCheckDate = shift;
	my $checkUpToDate = shift;
	my $data_interval_minutes = shift;
	my $maxDurationHoursWithoutData = shift; # file_expecting_interval_hours in database
	
	my $checkUpToDateTime = Time::Piece->strptime($checkUpToDate, "%Y-%m-%d %H:%M:%S");
	
	my $stSelectRecords = $dbh->prepare("SELECT station_name, value_date FROM record WHERE station_name = ? AND value_date > ? ORDER BY value_date");
	$stSelectRecords->execute($stationName, $lastCheckDate);
	my $previousDateString = $lastCheckDate;
	
	my $errors = 0;
	my $errorMsg = "";
	# Check that no record is missing (normally there should be a record every 15min)
	while (my @row = $stSelectRecords->fetchrow_array()) {
		my $recordDateString = $row[1];
		my $recordTime = Time::Piece->strptime($recordDateString, "%Y-%m-%d %H:%M:%S");
		
		if ($previousDateString eq "1970-01-01 00:00:00") {
			$previousDateString = $recordDateString;
		}
		my $previousTime = Time::Piece->strptime($previousDateString, "%Y-%m-%d %H:%M:%S");
		
		my $timeDiff = $recordTime - $previousTime;
		my $nbMissingData = ($timeDiff/60)/$data_interval_minutes - 1; # Every $data_interval_minutes min
		if ($nbMissingData > 0) {
			$errors++;
			$errorMsg .= int($nbMissingData+0.5)." données manquantes entre $previousDateString et $recordDateString\n";
		}
		
		$previousDateString = $recordDateString;
		
		if ($recordTime > $checkUpToDateTime) {
			# We have reached today midnight, wait tomorrow to check this day
			# We don't check data of current day as we haven't received it totally.
			last;
		}
	}
	# update lastCheckDate
	my $stUpdateLastCheckDate = $dbh->prepare("UPDATE station SET last_value_date_checked = ? WHERE name = ?");
	$stUpdateLastCheckDate->execute($previousDateString, $stationName);
	
	# Check that last record is not older than $maxDurationHoursWithoutData from $checkUpToDateTime
	my $durationMinutesSinceLastData = ($checkUpToDateTime - Time::Piece->strptime($previousDateString, "%Y-%m-%d %H:%M:%S")) / 60;
	if ($durationMinutesSinceLastData > $maxDurationHoursWithoutData
		&& ($dbh->selectrow_array("SELECT alert_sent FROM station WHERE name = ?", undef, $stationName) != 1)) {
		# No data has been received for a duration greater than the expected one
		# And the alert has not been sent already (once sent don't send it each time)
		$errors++;
		$errorMsg .= "Aucune donnée remontée pour la station '$stationName' depuis $previousDateString\n";
		# Set boolean in database to mute this alarm until data are back (don't send it each time)
		my $stUpdateAlertSent = $dbh->prepare("UPDATE station SET alert_sent = 1 WHERE name = ?");
		$stUpdateAlertSent->execute($stationName);
	}
	
	if ($errors > 0) {
		CreaCommons::sendAlarm($conf{"destMail"}, "Données manquantes pour la station '$stationName': \n$errorMsg");
	}
	
}

sub doCheck() {

	my $stSelectStations = $dbh->prepare("SELECT name, last_value_date_checked, file_expecting_interval_hours, data_interval_minutes, state FROM station");
	$stSelectStations->execute();
	while (my @row = $stSelectStations->fetchrow_array()) {
		my $state = $row[4];
		if ($state ne 'active')  {
			# Station is not active, no need to check if data are reported
			next;
		}
		
		my $lastCheckDate = $row[1];
		if (!defined($lastCheckDate) || $lastCheckDate =~ /^\s*$/) {
			$lastCheckDate = "1970-01-01 00:00:00"; # Default to epoch if no check has been done yet
		}
		
		my $file_expecting_interval_hours = $row[2];
		# Check that data are present up to (now - the configured number of hours) because of the delay to upload data 
		# (data are not uploaded every time they are stored in order to save battery. They are sent grouped)
		my $checkUpToDateTime = gmtime;
		$checkUpToDateTime -= $file_expecting_interval_hours*3600;
		my $checkUpToDate = $checkUpToDateTime->strftime("%Y-%m-%d %H:%M:%S");
		
		my $data_interval_minutes = $row[3];
		checkStation($row[0], $lastCheckDate, $checkUpToDate, $data_interval_minutes, $file_expecting_interval_hours);
	}
}


#------------------------------------

%conf = CreaCommons::loadConfig($configFilePath);

my $locker = Locker->new("alarms.lock");

connectToSqliteDb();

doCheck();

$dbh->disconnect() if $dbh;
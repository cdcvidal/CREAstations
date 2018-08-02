#!/usr/bin/perl

use File::Copy;
use File::Basename; 
use DBI;
use Locker;
use CreaCommons;
use Time::Piece;
use strict;

# Configuration
my %conf;

# Handle to sqlite db
my $dbh;

my $configFilePath = "conf.ini";

sub initSqliteDb() {
	$CreaCommons::logger->info("Initializing SQLite database '".$conf{"dbName"}."'...");
	
	if (!-e $conf{"dbName"}) {
		# Don't connect before checking if file exists else file will be created anyway without tables
		$dbh = DBI->connect("dbi:SQLite:dbname=".$conf{"dbName"}, "", "", {
			PrintError	=> 0,
			RaiseError	=> 1,
			AutoCommit	=> 1,
			sqlite_allow_multiple_statements => 1}) 
			or $CreaCommons::logger->logdie("Failed to create database '".$conf{"dbName"}."': $DBI::errstr");
			
		# Create DB		
		# value_date 			VARCHAR(19) => YYYY-MM-DD HH:MM:SS
		# inserted_date_utc		date when the record has been inserted in database
		#						(same format as value_date: VARCHAR(19) => YYYY-MM-DD HH:MM:SS)
		# alert_sent			1 if an alert has been sent to warn that the station has not uploaded data
		#						for a duration between the last data and now greater than the defined one;
		#						0 otherwise.
		# last_battery_value	battery value in Volts
		# last_battery_date		date of the last_battery_value (VARCHAR(19) => YYYY-MM-DD HH:MM:SS)
		#
		# data_interval_minutes				interval in minutes for the data (every 15 minutes for example). Used to 
		#									check continuity of reported data for alarm generation
		# file_expecting_interval_hours		Interval in hours where we can expect the file to be present. Used to 
		#									check that the station still reports data (in alarm generation)
		# state					State of the station. Can be:
		#						- active (the station is expected to report data)
		#						- inactive (the station is not used anymore)
		my $sqlCreate = <<'END_SQL';
			CREATE TABLE sensor_record(
				record_id		INTEGER,
				sensor_index	INTEGER,
				sensor_value	VARCHAR(100),
				PRIMARY KEY(record_id, sensor_index),
				FOREIGN KEY(record_id) REFERENCES record(id)
				);
		
			CREATE TABLE record(
				id INTEGER PRIMARY KEY,
				station_name 		VARCHAR(100),
				value_date 			VARCHAR(19),
				valid				INTEGER,
				is_ndvi				INTEGER,
				filename			TEXT,
				inserted_date_utc	VARCHAR(19)
				);
			CREATE INDEX record_station_name_idx ON record(station_name);
			CREATE INDEX record_value_date_idx ON record(value_date);
			CREATE INDEX record_filename_idx ON record(filename);
			
			CREATE TABLE station(
				id INTEGER PRIMARY KEY,
				name 							VARCHAR(100) UNIQUE,
				last_value_date_checked			VARCHAR(19) DEFAULT NULL,
				alert_sent						INTEGER DEFAULT 0,
				version							TEXT DEFAULT "V3",
				last_battery_value				REAL,
				last_battery_date				VARCHAR(19),
				data_interval_minutes			INTEGER DEFAULT 15,
				file_expecting_interval_hours	INTEGER DEFAULT 4,
				state							TEXT DEFAULT "active"
			);
			CREATE INDEX station_name_idx ON station(name);
END_SQL
		
		$dbh->do($sqlCreate) 
			or $CreaCommons::logger->logdie("Failed to create database '".$conf{"dbName"}."': $DBI::errstr");
			
		$CreaCommons::logger->info("SQLite database '".$conf{"dbName"}."' successfully created");

		# Close connection and reopen it with the standard options (without 'sqlite_allow_multiple_statements => 1')
		$dbh->disconnect();
	}
	
	$dbh = DBI->connect("dbi:SQLite:dbname=".$conf{"dbName"}, "", "", {
		PrintError	=> 0,
		RaiseError	=> 1,
		AutoCommit	=> 1}) 
		or $CreaCommons::logger->logdie("Failed to open database '".$conf{"dbName"}."': $DBI::errstr");
}

sub mergeRecord($$$$$){
	my $name = shift;
	my $sqlDate = shift;
	my $dataValid = shift;
	my $isNdvi = shift;
	my $filename = shift;
	
	my ($exists) = $dbh->selectrow_array(
		"SELECT id FROM record WHERE station_name=? AND value_date=? AND is_ndvi=?", undef, $name, $sqlDate, $isNdvi);
	if ($exists == 0) {
		$dbh->do("INSERT INTO record(station_name, value_date, valid, is_ndvi, filename, inserted_date_utc) VALUES (?, ?, ?, ?, ?,  datetime('now'))", undef, $name, $sqlDate, $dataValid, $isNdvi, $filename);
		return $dbh->last_insert_id("", "", "record", "");
	}
	else{
		$dbh->do("UPDATE record SET valid=?, is_ndvi=?, filename=?, inserted_date_utc=datetime('now') WHERE station_name=? AND value_date=? AND is_ndvi=?", undef, $dataValid, $isNdvi, $filename, $name, $sqlDate, $isNdvi);
		return $exists;
	}
}

sub mergeSensorRecord($$$){
	my $record_id = shift;
	my $sensorIndex = shift;
	my $sensorVal = shift;

	my ($exists) = $dbh->selectrow_array(
		"SELECT 1, record_id FROM sensor_record WHERE record_id=? AND sensor_index=?", undef, $record_id, $sensorIndex);
	if ($exists != 1) {
		$dbh->do("INSERT INTO sensor_record(record_id, sensor_index, sensor_value) VALUES (?, ?, ?)", undef, $record_id, $sensorIndex, $sensorVal);
	}
	else{
		$dbh->do("UPDATE sensor_record SET sensor_value=? WHERE record_id=? AND sensor_index=?", undef, $sensorVal, $record_id, $sensorIndex);
	}
}

sub parse($$) {
	my $filename = shift;
	my $limited = shift;
	my $errMsg = "";
	
	my $fileDateTime = undef;
	# Check date of file (Files are named: STATIONNAME_DDMMYYYY_HHMMSS.txt)
	# If file is older than a month, it should be ignored
	# because some stations resend old files sometimes.
	if ($filename =~ /.*_(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})(\d{2})\.txt/) {
		if ($limited == 1){
			$fileDateTime = Time::Piece->strptime("$3-$2-$1 $4:$5:$6", "%Y-%m-%d %H:%M:%S");
			my $nowDateTime = localtime;
			my $aMonthAgo = $nowDateTime->add_months(-1);
			if ($fileDateTime < $aMonthAgo) {
				# Ignore files older than a month
				CreaCommons::sendAlarm($conf{"destMail"}, "Le fichier '$filename' a été ignoré car ses données datent de plus d'un mois.");
				return -4;
			}
		}
	} else {
		CreaCommons::sendAlarm($conf{"destMail"}, "Le nom du fichier [$filename] ne respecte par la règle de nommage 'NomDeStation_JJMMAAAA_HHMMSS.txt'");
		return -3;
	}
	
	$CreaCommons::logger->info("Parsing $filename...");
	unless(open FILE, $filename) {
		CreaCommons::sendAlarm($conf{"destMail"}, "Impossible de lire le fichier '$filename': $!");
		return -1;
	}
	
	my $isNdvi = 0;
	if ($filename =~ "/NDVI/") {
		# No range check for NDVI files
		$CreaCommons::logger->info("NDVI file, no range check");
		$isNdvi = 1;
	}

	my $stationName = undef;
	if ($filename =~ /ST([^_]+)_.*/) {
		$stationName = $1;
	}
	elsif ($filename =~ /L?NV([^_]+)_.*/) {
		$stationName = $1;
		# No range check for NDVI files
		$CreaCommons::logger->info("NDVI file, no range check");
		$isNdvi = 1;
	}
	else {
		CreaCommons::sendAlarm($conf{"destMail"}, "Le nom du fichier [$filename] ne respecte par la règle de nommage 'NomDeStation_DATE.txt'");
		return -3;
	}
	
	# Get alert status and version of current station
	my ($exists, $alert_sentDatabase, $version, $last_battery_dateDatabase) = $dbh->selectrow_array(
				"SELECT 1, alert_sent, version, last_battery_date FROM station WHERE name=?", undef, $stationName);
	if ($exists != 1) {
		$CreaCommons::logger->info("Adding station '$stationName' to db stations");
		$dbh->do('INSERT INTO station(name) VALUES (?)', undef, $stationName);
	}
	
	my $storeBatteryLevel = 0;
	if ($isNdvi == 0 && ($version eq 'V3' || $version eq 'V2')) {
		# Stations in V3 and V2 send their battery level (in non NDVI files) so we can store it
		$storeBatteryLevel = 1;
	}
	
	# Begin transaction
	$dbh->begin_work;
	
	# my $stInsertRecord = $dbh->prepare("INSERT INTO record(station_name, value_date, valid, is_ndvi, filename, inserted_date_utc) VALUES (?, ?, ?, ?, ?,  datetime('now'))");
	# my $stInsertSensorRecord = $dbh->prepare('INSERT INTO sensor_record(record_id, sensor_index, sensor_value) VALUES (?, ?, ?)');
	
	my $lineNumber = 0;
	my $errorsInFile = 0;
	my $errorsTemperatureInFile = 0;
	my $dataInsertedInDb = 0;
	my $lastBatteryValue = undef;
	my $lastBatteryDate = undef;
	# 1 if the file date correspond to a date of a data; 0 otherwise
	my $fileDateOk = 0;
	my $lastDataDate = undef;
	
	# test de la date du dernier relevé
	
	while(<FILE>) {
		my ($num, $date, $hour, $sensorsRaw);
		$lineNumber++;
		if ($_ =~ /^([^;]+);([^;]+);([^;]+);(.*)$/) {
			($num, $date, $hour, $sensorsRaw) = ($1, $2, $3, $4);
		} elsif ($_ eq '+++') {
			# ignore line with '+++', as some stations end the files with this pattern
			next;
		} else {
			$errorsInFile++;
			$errorsTemperatureInFile = 0;
			$errMsg .= ("Ligne '$_' invalide dans le fichier '$filename' à la ligne '". $lineNumber . "'. La ligne a été ignorée.\n");
			next;
		}
		
		if ($lineNumber == 1 && ($date eq 'X' || $hour eq 'X')) {
			# First line without data, skip it
			next;
		}
		
		my $dataValid = 1;
		my @sensors = split(';', $sensorsRaw);
		if ($isNdvi == 0) {	
			# Check range of values for non NDVI files
			my $index = 0;
			for my $sensorVal (@sensors) {
				$index++;
				# Check that sensor data is between -40 and +50°C
				# Except for first line which may not contain data
				# and for last sensor data which is not a temperature and so has not to be checked
				if ($lineNumber > 1 && $index != scalar(@sensors) && ($sensorVal < -40 || $sensorVal > 50)) {
					$errMsg .= ("Valeur #$index du capteur [$num] hors limite : [$sensorVal] ($date $hour à la ligne $lineNumber)\n");
					$dataValid = 0;
					$errorsInFile++;
					$errorsTemperatureInFile = 1;
				}
			}
		}
		
		# Insert in database
		# Convert from DD/MM/YY HH:MM:SS to YYYY-MM-DD HH:MM:SS (Years are in 2000's so 20+YY)
		my $sqlDate = "$date $hour";
		if ($sqlDate =~ /(\d+)\/(\d+)\/(\d+) (\d+:\d+:\d+)/) {
			my $fullHour = $4;
			$sqlDate = "20$3-$2-$1 $4";
			my $record_id = mergeRecord($num, $sqlDate, $dataValid, $isNdvi, $filename);
			$dataInsertedInDb++;
			my $sensorIndex = 0;
			for my $sensorVal (@sensors) {
				if ($sensorVal =~ /\?\?\.\?\?/) {
					# Bug of the device: may return ??.?? for value 0
					$sensorVal = '0';
				}
				my $sensorRecord = mergeSensorRecord($record_id, $sensorIndex, $sensorVal);
				$sensorIndex++;
			}
			
			# Check that date of filename is between date of measure +/- 10 minutes
			my $sqlDateTime = Time::Piece->strptime($sqlDate, "%Y-%m-%d %H:%M:%S");
			if ($fileDateTime > ($sqlDateTime - 600) && $fileDateTime < ($sqlDateTime + 600)) {
				$fileDateOk = 1;
			}
			$lastDataDate = $sqlDate;
			
			# For non NDVI files of stations in version 3, the battery level is reported at each hour with 15 and 45 minutes (hh:15:00 and hh:45:00 => hh:m5:00)
			# Battery level is reported as dV (deciVolts) so divide it by 10 to have it in V (Volts)
			if ($storeBatteryLevel != 0 && $fullHour =~ /\d+:\d5:\d+/){
				if (!($sensors[4] =~ /\+?\d+/)) {
					$errMsg .= ("Niveau de batterie '".$sensors[4]."' invalide pour la station '$stationName' à la date $sqlDate\n");
				} 
				else {
					$lastBatteryValue = $sensors[4]/10;
					$lastBatteryDate = $sqlDate;
				}
			}
			
			if (!($num eq $stationName)) {
				$errMsg .= ("Données d'une autre station ('$num') dans le fichier de la station '$stationName'\n");
			}
		} else {
			$errMsg .= ("Date '$sqlDate' invalide dans le fichier '$filename' à la ligne '". $lineNumber . "'. La ligne a été ignorée.\n");
			$errorsInFile++;
			$errorsTemperatureInFile = 0;
		}
	}
	
	if ($alert_sentDatabase == 1 && $dataInsertedInDb > 0) {
		# Reset flag as new data have been received
		my $stUpdateAlertSent = $dbh->prepare("UPDATE station SET alert_sent = 0 WHERE name = ?");
		$stUpdateAlertSent->execute($stationName);
	}
	
	# update battery level if we have found a battery level value in file
	# and if the date of this battery level is newer than the one stored in database
	# (We do a string comparison of the date as they are in the format YYYY-MM-DD HH:MM:SS)
	if (defined($lastBatteryValue) && ($lastBatteryDate gt $last_battery_dateDatabase)) {
		$dbh->do("UPDATE station SET last_battery_value = ?, last_battery_date = ? WHERE name = ?", 
			undef, $lastBatteryValue, $lastBatteryDate, $stationName);
	}
	
	# COMMIT
	$dbh->commit;
	
	close FILE;
	
	if ($dataInsertedInDb > 0 && $fileDateOk == 0){
		# $errMsg .= ("La date du nom du fichier '$filename' ne correspond pas aux dates des données (date de la dernière donnée du fichier: $lastDataDate).\n");
		# $errorsInFile++;
		# $errorsTemperatureInFile = 0;
	}
	
	if ($errorsInFile > 0 && $errorsTemperatureInFile == 0) {
		CreaCommons::sendAlarm($conf{"destMail"}, "Erreurs détectées dans le fichier '$filename': \n$errMsg");
		return -2;
	}
	
	return 0;
}

sub processDir($$$$) {
	my $dir = shift;
	my $limited = 1;
	
	if ($dir == $conf{"inputDirManual"}){
		$limited = 0;
	}
	
	my $outProcessed = shift;
	my $outFailed = shift;
	my $outFailedCopy = shift;
		
	if (!-d $outProcessed) {
		mkdir $outProcessed;
	}
	if (!-d $outFailed) {
		mkdir $outFailed;
	}
	if (!-d $outFailedCopy) {
		mkdir $outFailedCopy;
	}
	
	unless(opendir(DIR, $dir)) {
		$CreaCommons::logger->logwarn("Failed to open directory [$dir]: $!");
		return -1;
	}

	my @files = readdir(DIR);
	for my $arg (@files) {
		my $filename = "$dir/$arg";
		
		# Ignore ".", ".." and "cfg" directories
		if (-d $filename && !($arg eq ".") && !($arg eq "..") && !($arg eq "cfg")) {
			$CreaCommons::logger->info("Entering directory [$filename]...");
			processDir($filename, "$outProcessed/$arg", "$outFailed/$arg", "$outFailedCopy/$arg");
			$CreaCommons::logger->info("End for directory [$filename]...");
			next;
		}
		
		if (!($arg =~ /\.txt$/)) {
			##print("$arg don't match .txt\n");
			next;
		}
		
		my $ret;
		$CreaCommons::logger->info("Processing $filename...");
		
		# Check if file has already been parsed
		# my $nbRecordInDbFromFile = $dbh->selectrow_array("SELECT COUNT(id) FROM record WHERE filename = ?", undef, $filename);
		# if ($nbRecordInDbFromFile > 0) {
		# 	CreaCommons::sendAlarm($conf{"destMail"}, "Le fichier '$filename' a déjà été traité précédemment.");
		# 	$ret = -2;
		# } else {
		# 	$ret = parse($filename);
		# }
		
		$ret = parse($filename, $limited);
		
		my ($name,$path,$suffix) = fileparse($filename,"");
		if ($ret == 0 || $ret == -4) {
			# File has been successfully parsed or should be ignored without copying it to the error directory
			moveAndRenameIfExist($filename, "$outProcessed/$name");
		}
		elsif ($ret == -2 || $ret == -3) {
			# Invalid value for sensors
			copyAndRenameIfExist($filename, "$outFailed/$name");
			moveAndRenameIfExist($filename, "$outProcessed/$name");
		}
		elsif ($ret == -1) {
			# Failed to open file
			# TODO: what to do? let the file and retry another time?
		}
		else {
			copyAndRenameIfExist($filename, "$outFailedCopy/$name");
			CreaCommons::sendAlarm($conf{"destMail"}, "Le traitement du fichier '$filename' a rencontré une erreur inconnue: $ret");
		}
	}
	
	return 1;
}

#------------------------------------

sub copyAndRenameIfExist($$) {
	my $src = shift;
	my $dest = shift;
	my $finalDest = $dest;
	
	if (-e $dest) {
		my $i = 1;
		while(-e "$dest-$i.txt") {
			$i++;
		}
		$finalDest = "$dest-$i.txt";
	}
	
	copy($src, $finalDest) or $CreaCommons::logger->logwarn("Failed to copy file from '$src' to '$finalDest': $!");
}

sub moveAndRenameIfExist($$) {
	my $src = shift;
	my $dest = shift;
	my $finalDest = $dest;
	
	if (-e $dest) {
		my $i = 1;
		while(-e "$dest-$i.txt") {
			$i++;
		}
		$finalDest = "$dest-$i.txt";
	}
	
	move($src, $finalDest) or $CreaCommons::logger->logwarn("Failed to move file from '$src' to '$finalDest': $!");
}
#------------------------------------

%conf = CreaCommons::loadConfig($configFilePath);

my $locker = Locker->new("collecte.lock");

initSqliteDb();

processDir($conf{"inputDir"}, $conf{"outputDirProcessed"}, $conf{"outputDirFailed"}, $conf{"outputDirFailedCopy"});
processDir($conf{"inputDirManual"}, $conf{"outputDirProcessed"}, $conf{"outputDirFailed"}, $conf{"outputDirFailedCopy"});

$dbh->disconnect() if $dbh;


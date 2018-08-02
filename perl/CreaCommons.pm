#!/usr/bin/perl

package CreaCommons;

use File::Copy;
use File::Basename; 
use Config::IniFiles;
use DBI;
use Locker;
use MIME::Lite;
use Log::Log4perl qw(:easy);
use strict;

# Init Logging
Log::Log4perl::init("log4perl.conf");
our $logger = Log::Log4perl::get_logger();

sub getIniParam($$$$$) {
	my $cfg = shift;
	my $section = shift;
	my $parameter = shift;
	my $required = shift;
	my $configFilePath = shift;
	
	my $val = $cfg->val($section, $parameter);
	
	if ($required && !defined($val)) {
		$logger->logdie("Invalid configuration file [$configFilePath]: missing '$parameter' in section '$section'");
	} elsif ($required && $val =~ /^\s*$/) {
		$logger->logdie("Invalid configuration file [$configFilePath]: empty '$parameter' in section '$section'");
	}
	
	return $val;
}

sub loadConfig($) {
	my $configFilePath = shift;
	# Fields read from configuration file
	my $inputDir;
	my $inputDirManual;
	my $outputDirProcessed;
	my $outputDirFailed;
	my $outputDirFailedCopy;
	my $dbName;
	my $dbPath;
	my $destMail;
	
	my $backupFtpHost;
	my $backupFtpLogin;
	my $backupFtpPassword;
	my $backupFtpDir;

	my $cfg = Config::IniFiles->new( -file => $configFilePath );
	$inputDir = getIniParam($cfg, 'InputDirectory', 'path', 1, $configFilePath);
	$inputDirManual = getIniParam($cfg, 'InputDirectory', 'pathManual', 1, $configFilePath);
	
	$outputDirProcessed = getIniParam($cfg, 'OutputDirectory', 'pathProcessed', 1, $configFilePath);
	$outputDirFailed = getIniParam($cfg, 'OutputDirectory', 'pathFailed', 1, $configFilePath);
	$outputDirFailedCopy = getIniParam($cfg, 'OutputDirectory', 'pathFailedCopy', 1, $configFilePath);
	
	$dbName = getIniParam($cfg, 'Sqlite', 'dbname', 1, $configFilePath);
	$dbPath = getIniParam($cfg, 'Sqlite', 'dbpath', 1, $configFilePath);
	
	$destMail = getIniParam($cfg, 'Alerts', 'destination-mail', 1, $configFilePath);
	
	# Backup FTP parameters
	$backupFtpHost = getIniParam($cfg, 'BackupFTP', 'host', 1, $configFilePath);
	$backupFtpLogin = getIniParam($cfg, 'BackupFTP', 'login', 1, $configFilePath);
	$backupFtpPassword = getIniParam($cfg, 'BackupFTP', 'pass', 1, $configFilePath);
	$backupFtpDir = getIniParam($cfg, 'BackupFTP', 'ftpDir', 1, $configFilePath);
	
	return (
			inputDir => $inputDir,
			inputDirManual => $inputDirManual,
			outputDirProcessed => $outputDirProcessed,
			outputDirFailed => $outputDirFailed,
			outputDirFailedCopy => $outputDirFailedCopy,
			dbName => $dbName,
			dbPath => $dbPath,
			destMail => $destMail,
			backupFtpHost => $backupFtpHost,
			backupFtpLogin => $backupFtpLogin,
			backupFtpPassword => $backupFtpPassword,
			backupFtpDir => $backupFtpDir
		);
}

sub sendAlarm($$) {
	my $destMail = shift;
	my $msg = shift;
	$msg =~ s/\n/\n\t/g;
	$logger->warn("Erreur: ". $msg);
	
	my $to = $destMail;
	my $cc = '';
	my $from = 'crea@vps144491.ovh.net';
	my $subject = 'CREA: Alertes';
	my $message = "Bonjour,\nL'alerte suivante vient d'être levée:\n$msg\n-------\nCordialement";

	my $mail = MIME::Lite->new(
					 From     => $from,
					 To       => $to,
					 Cc       => $cc,
					 Subject  => $subject,
					 Data     => $message
					 );
					 
	$mail->send or $logger->warn("Failed to send mail: $!");
}

1;

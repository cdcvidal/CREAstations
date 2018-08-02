#!/usr/bin/perl

use Locker;
use CreaCommons;
use Net::FTP;
use Time::Piece;
use Path::Class;
use strict;
use warnings;

# Configuration
my %conf;

my $configFilePath = "conf.ini";

sub backupFailed($) {
	my $msg = shift;
	$CreaCommons::logger->fatal($msg);
	
	CreaCommons::sendAlarm($conf{"destMail"}, "Echec de la sauvegarde des données sur le serveur FTP de backup: $msg");
	
	die($msg);
}

sub backupFtp() {	
	if (!(-d "archive")) {
		mkdir "archive";
	}
	# Archive
	my $sourceDir = dir($conf{"outputDirProcessed"});
	my $backupArchive = "archive/collecte-".localtime->strftime("%Y-%m-%d_%H-%M-%S").".tar.gz";
	my $syscmd = "tar -czf $backupArchive -C ".$sourceDir->parent." ".$sourceDir->basename;
	system($syscmd);
	if (!(-e $backupArchive)) {
		backupFailed("Impossible de créer l'archive '$backupArchive' depuis le dossier '".$conf{"outputDirProcessed"}."'");
	}
	
	# Send to FTP
	my $ftp = Net::FTP->new($conf{"backupFtpHost"})
		or backupFailed("Impossible de se connecter au serveur FTP de sauvegarde: '".$conf{"backupFtpHost"}."': $@");
	
	$ftp->login($conf{"backupFtpLogin"}, $conf{"backupFtpPassword"})
		or backupFailed("Impossible de se connecter au serveur FTP de sauvegarde: '".$conf{"backupFtpHost"}."' avec le login '".$conf{"backupFtpLogin"}."': ". $ftp->message);
	
	$ftp->cwd($conf{"backupFtpDir"})
		or backupFailed("Impossible d'aller dans le dossier '".$conf{"backupFtpDir"}."' sur le serveur FTP de sauvegarde : '".$conf{"backupFtpHost"}."': ". $ftp->message);
	
	$ftp->binary();
	
	$ftp->put($backupArchive)
		or backupFailed("Impossible d'envoyer le fichier '$backupArchive' sur le serveur FTP de sauvegarde: '".$conf{"backupFtpHost"}."': ". $ftp->message);
	
	$ftp->quit();
	
	# Delete archive file on current machine
	unlink($backupArchive);
}



#------------------------------------

%conf = CreaCommons::loadConfig($configFilePath);

my $locker = Locker->new("backup.lock");

backupFtp();

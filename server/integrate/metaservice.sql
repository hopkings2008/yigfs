DROP TABLE IF EXISTS `dir`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dir` (
  `ino` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `generation` bigint(20) UNSIGNED DEFAULT 0,
  `region` varchar(255) DEFAULT NULL,
  `bucket_name` varchar(255) DEFAULT NULL,
  `parent_ino` bigint(20) UNSIGNED DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `size` bigint(20) UNSIGNED DEFAULT NULL,
  `type` int(11) UNSIGNED DEFAULT NULL,
  `owner` varchar(255) DEFAULT NULL,
  `ctime` datetime DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
  `atime` datetime DEFAULT NULL,
  `perm` int(11) UNSIGNED DEFAULT NULL,
  `nlink` int(11) UNSIGNED DEFAULT NULL,
  `uid` int(11) UNSIGNED DEFAULT NULL,
  `gid` int(11) UNSIGNED DEFAULT NULL,
   PRIMARY KEY (`ino`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
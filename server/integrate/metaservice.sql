DROP TABLE IF EXISTS `file`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `file` (
  `ino` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `generation` bigint(20) UNSIGNED DEFAULT 0,
  `region` varchar(255) DEFAULT "cn-bj-1",
  `bucket_name` varchar(255) DEFAULT NULL,
  `parent_ino` bigint(20) UNSIGNED DEFAULT 1,
  `file_name` varchar(255) DEFAULT NULL,
  `size` bigint(20) UNSIGNED DEFAULT 0,
  `type` int(11) UNSIGNED DEFAULT 1,
  `ctime` datetime DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
  `atime` datetime DEFAULT NULL,
  `perm` int(11) UNSIGNED DEFAULT 644,
  `nlink` int(11) UNSIGNED DEFAULT 0,
  `uid` int(11) UNSIGNED DEFAULT 0,
  `gid` int(11) UNSIGNED DEFAULT 0,
  `blocks` int(11) UNSIGNED DEFAULT 0,
   UNIQUE KEY `rowkey` (`ino`, `generation`, `region`, `bucket_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `zone`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `zone` (
  `id` varchar(255) DEFAULT NULL,
  `region` varchar(255) DEFAULT "cn-bj-1",
  `bucket_name` varchar(255) DEFAULT NULL,
  `machine` varchar(255) DEFAULT NULL,
  `status` tinyint(1) DEFAULT 1,
  `weight` int(11) UNSIGNED DEFAULT 0,
  `ctime` datetime DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
   UNIQUE KEY `rowkey` (`id`, `region`, `bucket_name`, `machine`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `file_leader`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `file_leader` (
  `zone_id` varchar(255) DEFAULT NULL,
  `region` varchar(255) DEFAULT "cn-bj-1",
  `bucket_name` varchar(255) DEFAULT NULL,
  `ino` bigint(20) UNSIGNED DEFAULT 0,
  `generation` bigint(20) UNSIGNED DEFAULT 0,
  `leader` varchar(255) DEFAULT NULL,
  `ctime` datetime DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
  `is_deleted` tinyint(1) DEFAULT 0,
   UNIQUE KEY `rowkey` (`zone_id`, `region`, `bucket_name`, `ino`, `generation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `segment_leader`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `segment_leader` (
  `zone_id` varchar(255) DEFAULT NULL,
  `region` varchar(255) DEFAULT "cn-bj-1",
  `bucket_name` varchar(255) DEFAULT NULL,
  `seg_id0` bigint(20) UNSIGNED DEFAULT 0,
  `seg_id1` bigint(20) UNSIGNED DEFAULT 0,
  `leader` varchar(255) DEFAULT NULL,
  `ctime` datetime DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
  `is_deleted` tinyint(1) DEFAULT 0,
   UNIQUE KEY `rowkey` (`zone_id`, `region`, `bucket_name`, `seg_id0`, `seg_id1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `block`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `block` (
  `region` varchar(255) DEFAULT "cn-bj-1",
  `bucket_name` varchar(255) DEFAULT NULL,
  `ino` bigint(20) UNSIGNED DEFAULT 0,
  `generation` bigint(20) UNSIGNED DEFAULT 0,
  `seg_id0` bigint(20) UNSIGNED DEFAULT 0,
  `seg_id1` bigint(20) UNSIGNED DEFAULT 0,
  `block_id` bigint(20) DEFAULT 0,
  `size` int(11) DEFAULT 0,
  `offset` bigint(20) DEFAULT 0,
  `seg_start_addr` bigint(20) DEFAULT 0,
  `seg_end_addr` bigint(20) DEFAULT 0,
  `ctime` datetime DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
  `is_deleted` tinyint(1) DEFAULT 0,
   UNIQUE KEY `rowkey` (`region`, `bucket_name`, `ino`, `generation`, `seg_id0`, `seg_id1`, `block_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

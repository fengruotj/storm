/*
Navicat MySQL Data Transfer

Source Server         : 华科ubuntu2
Source Server Version : 50505
Source Host           : ubuntu2:3306
Source Database       : benchmark

Target Server Type    : MYSQL
Target Server Version : 50505
File Encoding         : 65001

Date: 2018-06-22 08:52:10
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for t_systemlatency
-- ----------------------------
DROP TABLE IF EXISTS `t_systemlatency`;
CREATE TABLE `t_systemlatency` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `taskid` int(11) NOT NULL,
  `communicationTime` bigint(20) NOT NULL,
  `computeTime` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12732 DEFAULT CHARSET=utf8mb4;

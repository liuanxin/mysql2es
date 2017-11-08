
DROP TABLE IF EXISTS `t_product`;
CREATE TABLE IF NOT EXISTS `t_product` (
  `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,

  `name` VARCHAR(128) NOT NULL COMMENT '商品名',

  `create_time` DATETIME NOT NULL COMMENT '创建时间',
  `update_time` DATETIME NOT NULL COMMENT '更新时间',
  `is_delete` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '是否删除(1.已删除, 0.未删除). 默认是 0',
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT='商品';


DROP TABLE IF EXISTS `t_content`;
CREATE TABLE IF NOT EXISTS `t_content` (
  `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,

  `type_id` BIGINT(20) UNSIGNED NOT NULL COMMENT '类型 id(1.散文, 2.短篇, 3.其他 etc...)',
  `url` VARCHAR(512) NOT NULL COMMENT '数据来源',
  `date` DATETIME NOT NULL COMMENT '发布日期',
  `title` VARCHAR(256) NOT NULL COMMENT '标题',
  `content` LONGTEXT NOT NULL COMMENT '内容',

  `create_time` DATETIME NOT NULL COMMENT '创建时间',
  `update_time` DATETIME NOT NULL COMMENT '更新时间',
  `is_delete` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '是否删除(1.已删除, 0.未删除). 默认是 0',
  PRIMARY KEY (`id`, `type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COMMENT='内容';

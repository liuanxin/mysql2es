
DROP TABLE IF EXISTS `t_product_info`;
CREATE TABLE IF NOT EXISTS `t_product_info` (
  `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(64) NOT NULL COMMENT '商品名',

  `create_time` DATETIME NOT NULL COMMENT '创建时间',
  `update_time` DATETIME NOT NULL COMMENT '更新时间',
  `is_delete` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '1 表示已删除',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品表';


DROP TABLE IF EXISTS `t_common_content`;
CREATE TABLE IF NOT EXISTS `t_common_content` (
  `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `type_id` INT(10) UNSIGNED NOT NULL COMMENT '类型 id(1.散文, 2.短篇, 3.其他 etc...)',
  `url` VARCHAR(512) NOT NULL COMMENT '数据来源',
  `date` DATETIME NOT NULL COMMENT '发布日期',
  `title` VARCHAR(256) NOT NULL COMMENT '标题',
  `content` LONGTEXT NOT NULL COMMENT '内容',

  `create_time` DATETIME NOT NULL COMMENT '创建时间',
  `update_time` DATETIME NOT NULL COMMENT '更新时间',
  `is_delete` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '1 表示已删除',
  PRIMARY KEY (`id`, `type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COMMENT='内容表';

# concurrent-mysql

Evaluating different strategies for handling concurrent updates.

1. Create this table in MySQL:

``` sql
CREATE TABLE `T` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `count` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
```

2. Adjust script values numConcurrent and strategiesToTry.
3. Run with `DB_PASSWORD=password node index.js` and examine output for which
   strategies were correct or incorrect.

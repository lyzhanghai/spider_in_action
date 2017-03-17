
PRAGMA foreign_keys = false;

-- ----------------------------
--  Table structure for content
-- ----------------------------
DROP TABLE IF EXISTS "content";
CREATE TABLE 'content' ('id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 'url' VARCHAR NOT NULL, 'content' TEXT NOT NULL);
INSERT INTO "main".sqlite_sequence (name, seq) VALUES ("content", '0');

-- ----------------------------
--  Indexes structure for table content
-- ----------------------------
CREATE INDEX "idx_url" ON content ("url");

-- ----------------------------
--  Table structure for keyword2url
-- ----------------------------
DROP TABLE IF EXISTS "keyword2url";
CREATE TABLE "keyword2url" (
	 "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	 "keyword" text NOT NULL,
	 "url" text NOT NULL
);
INSERT INTO "main".sqlite_sequence (name, seq) VALUES ("keyword2url", '0');

-- ----------------------------
--  Indexes structure for table keyword2url
-- ----------------------------
CREATE INDEX "idx_keyword" ON keyword2url ("keyword");

CREATE TABLE "failed_url" (
	 "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	 "url" text NOT NULL,
	 "content" text NOT NULL,
	 "failed_reason" text NOT NULL
);

PRAGMA foreign_keys = true;
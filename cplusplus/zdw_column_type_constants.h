/**
 * Copyright 2019 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

#ifndef ZDW_COLUMN_TYPE_CONSTANTS_HPP
#define ZDW_COLUMN_TYPE_CONSTANTS_HPP

//these column type values are stored in the ZDW file schema -- do not modify
#define VARCHAR    (0)
#define TEXT       (1)
#define DATETIME   (2)
#define CHAR_2     (3)
#define VISID_LOW  (4) //before version 8
#define VISID_HIGH (5) // "
#define CHAR       (6)
#define TINY       (7)
#define SHORT      (8)
#define LONG       (9)
#define LONGLONG   (10)
#define DECIMAL    (11)
#define TINY_SIGNED     (12)
#define SHORT_SIGNED    (13)
#define LONG_SIGNED     (14)
#define LONGLONG_SIGNED (15)
#define TINYTEXT   (16)
#define MEDIUMTEXT   (17) //added in version 10
#define LONGTEXT   (18)
//virtual column values are used only in memory (i.e., not stored in ZDW files) and may be modified
#define VIRTUAL_EXPORT_FILE_BASENAME (64)
#define VIRTUAL_EXPORT_ROW (65)

#endif

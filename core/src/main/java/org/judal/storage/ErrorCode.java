package org.judal.storage;

/*
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

/**
 * <p>Authentication error codes.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public enum ErrorCode {

 SUCCESS                     (0),
 WORKAREA_MAY_NOT_BE_NULL    (1001),
 WORKAREA_MAY_NOT_BE_EMPTY   (1002),
 WORKAREA_ALREADY_REGISTERED (1003),
 WORKAREA_NOT_FOUND          (1004),
 PASSWORD_MAY_NOT_BE_NULL    (1005),
 PASSWORD_MAY_NOT_BE_EMPTY   (1006),
 PASSWORD_MISMATCH           (1007),
 EMAIL_MAY_NOT_BE_NULL       (1008),
 EMAIL_IS_NOT_VALID          (1009),
 EMAIL_ALREADY_EXISTS        (1010),
 USER_MAY_NOT_BE_NULL        (1011),
 USER_MAY_NOT_BE_EMPTY       (1012),   
 USER_ALREADY_EXISTS         (1013),
 USER_NOT_FOUND              (1014),
 SECURITYTOKEN_INVALID       (1015),
 CONFIRMATION_KEY_INVALID    (1016),
 ACCOUNT_DEACTIVATED         (1017),
 SESSION_EXPIRED             (1018),

 DATABASE_EXCEPTION          (8000),
 ILLEGALARGUMENT_EXCEPTION   (8001),
 IO_EXCEPTION      		     (8002),
 FILENOTFOUND_EXCEPTION      (8003),

 UNKNOWN_ERROR                (666);
 	   
 private final int iErrCode;

 ErrorCode (int errCode) {
   iErrCode = errCode;
 }

 public String toString() {
 	return String.valueOf(iErrCode);
 }

 public final int intValue() {
 	return iErrCode;
 }

}

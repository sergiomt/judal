/*
  Copyright (C) 2003-2014 KnowGate All rights reserved.

  Redistribution and use in source and binary forms,
  with or without modification, are permitted according
  to the terms of GNU Lesser General Public License 3
  provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
  3. Neither the name of the author nor the names of its contributors
     may be used to endorse or promote products derived from this software
     without specific prior written permission.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

*/
// ************************************************************
// Almacenamiento interno de datos del nodo ACTION para cada
// ROWSET definido en XML

package org.judal.datacopy;

public class DataRowSet {

  public DataRowSet() {
    FieldList = "*"; // Por defecto se leen todos los campos
    OriginTable = TargetTable = JoinTables = WhereClause = EraseClause = null;
  }

  public DataRowSet(String sOriginTable, String sTargetTable, String sJoinTables, String sWhereClause, String sEraseClause) {
    OriginTable = sOriginTable;
    TargetTable = sTargetTable;
    JoinTables = sJoinTables;
    WhereClause = sWhereClause;
    EraseClause = sEraseClause;
  }

  public String FieldList;   // Lista de campos (sólo si el nodo <FIELDLIST> existe en XML
  public String OriginTable; // Tabla Origen
  public String TargetTable; // Tabla Destino
  public String JoinTables;  // Tablas de JOIN (actualmente no se utiliza)
  public String WhereClause; // Claúsula WHERE
  public String EraseClause; // Claúsula de borrado
} // DataRowSet
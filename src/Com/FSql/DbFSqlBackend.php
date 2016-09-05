<?php
/**
 * Created by PhpStorm.
 * User: matthes
 * Date: 05.09.16
 * Time: 18:17
 */


    namespace DbAkl\Com\FSql;


    interface DbFSqlFackend {

        public function getTableName(string $tableOrClassName) : string;

        public function escapeString (string $input) : string;


    }

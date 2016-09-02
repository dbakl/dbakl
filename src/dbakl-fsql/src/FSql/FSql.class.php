<?php
/**
 * Created by PhpStorm.
 * User: matthes
 * Date: 18.09.14
 * Time: 17:03
 */


    namespace DbAkl\Com\FSql;


    use gis\core\exception\GisException;
    use gis\core\exception\NoDataException;
    use gis\db\core\Connection;
    use gis\db\driver\mysql\MySqlResult;
    use gis\db\exception\DbError;
    use gis\db\opt\edbc\core\EdbcEntity;
    use gis\db\opt\orm\core\Kernel;
    use gis\db\opt\orm\core\OrmResolver;
    use gis\db\opt\orm\exception\InvalidPropertyNameException;
    use gis\db\opt\orm\LateLoadingResult;
    use gis\db\opt\orm\PersistenceManager;
    use gis\db\RawSql;
    use gis\db\Sql;

    class FSql {

        const T_SELECT = "SELECT";
        const T_DELETE = "DELETE";
        const T_INSERT = "INSERT";
        const T_UPDATE = "UPDATE";

        private $mType;

        /**
         * @var Connection
         */
        private $mCon;
        private $mPm;

        public function __construct (Connection $con, PersistenceManager $pm = NULL) {
            $this->mCon = $con;
            $this->mPm = $pm;
        }


        private $mTable;


        /**
         * @param $table
         * @param null $id
         *
         * @return $this
         * @throws InvalidPropertyNameException
         */
        public function update ($tableOrClassName, $id=NULL) {
            if ($this->mType === NULL)
                $this->mType = self::T_UPDATE;
            $this->mTable = $this->getTableName($tableOrClassName);
            if ($id !== NULL)
                $this->where("id=?", $id);
            return $this;
        }

        /**
         * @param $table
         * @param null $id
         *
         * @return $this
         * @throws InvalidPropertyNameException
         */
        public function delete ($tableOrClassName, $id=NULL) {
            if ($this->mType === NULL)
                $this->mType = self::T_DELETE;
            $this->mTable = $this->getTableName($tableOrClassName);
            if ($id !== NULL)
                $this->where("id=?", $id);
            return $this;
        }

        /**
         * @param $table
         *
         * @return $this
         */
        public function insert ($tableOrClassName) {
            if ($this->mType === NULL)
                $this->mType = self::T_INSERT;
            $this->mTable = $this->getTableName($tableOrClassName);
            return $this;
        }


        private $mSet = [];

        /**
         * @param $key
         * @param $val
         *
         * @return $this
         */
        public function set ($key, $val) {
            $this->mSet[] = [$key, $val];
            return $this;
        }

        /**
         * @param $table
         *
         * @return $this
         */
        public function from ($tableOrClassName, $id=NULL) {

            if ($this->mType === NULL)
                $this->mType = self::T_SELECT;
            $this->mTable = $this->getTableName($tableOrClassName);
            if ($id !== NULL)
                $this->where("id=?", $id);
            return $this;
        }


        private $mSelects = [];



        /**
         * @param $col1
         * @param null $col2
         *
         * @return $this
         */
        public function select ($col1, $col2=NULL) {
            if ($this->mType === NULL)
                $this->mType = self::T_SELECT;
            if ($col1 === NULL) {
                $this->mSelects = [];
                return $this;
            }
            foreach (func_get_args() as $arg)
                $this->mSelects[] = $arg;
            return $this;
        }


        public $mJoins = [];



        public function getTableName ($tableOrClassName) {
            try {
                $def = Kernel::GetClassDefinition($tableOrClassName);
                $tableName = $def->tableName;
            } catch ( NoDataException $e) {
                $tableName = $tableOrClassName;
            }
            return $tableName;
        }





        /**
         * @param $table
         * @param null $on
         * @return $this
         */
        public function leftJoin ($tableOrClassName, $on=NULL) {
            $tableName = $this->getTableName($tableOrClassName);
            $this->mJoins[] = "LEFT JOIN {$tableName} ON $on";
            return $this;
        }

        /**
         * @param $table
         * @param null $on
         * @return $this
         */
        public function innerJoin ($tableOrClassName, $on=NULL) {
            $tableName = $this->getTableName($tableOrClassName);
            $this->mJoins[] = "INNER JOIN {$tableName} ON $on";
            return $this;
        }


        private $mWheres = [];


        /**
         * @param $field
         * @param null $val1
         * @param null $val2
         *
         * @return $this
         * @throws InvalidPropertyNameException
         */
        public function where ($query, $val1=NULL, $val2=NULL) {
            if ($query === NULL) {
                $this->mWheres = [];
                return $this;
            }

            preg_match_all("/(\\?\\?|\\?)/", $query, $matches);
            $args = func_get_args();
            foreach ($matches as $index => $match) {

                if (@$match[$index] == "??" && ! is_array (@$args[$index+1]))
                    throw new \InvalidArgumentException("Argument " . ($index+1) . " must be array");
                if (@$match[$index] == "?" && is_array(@$args[$index+1]))
                    throw new \InvalidArgumentException("Argument " . ($index+1) . " must not be array");
            }

            $this->mWheres[] = func_get_args(); // Includes $query on Index 0
            return $this;
        }


        private $mLimit = NULL;


        /**
         * @param $limit
         *
         * @return $this
         */
        public function limit ($limit) {
            if ($limit !== NULL)
                $this->mLimit = (int)$limit;
            return $this;
        }

        private $mOffset = NULL;

        /**
         * @param $offset
         *
         * @return $this
         */
        public function offset ($offset) {
            if ($offset !== NULL)
                $this->mOffset = (int)$offset;
            return $this;
        }

        private $mOrderBy = [];

        /**
         * @param $clause
         *
         * @return $this
         * @throws DbError
         */
        public function orderBy ($clause, $desc=FALSE) {
            if ($clause === NULL) {
                $this->mOrderBy = [];
                return $this;
            }
            if ( ! preg_match ("/^[a-z0-9\\.\\_\\-\\(\\)]+$/mi", $clause))
                throw new DbError("Invalid orderBy clause: '$clause'");
            if ($desc === TRUE)
                $clause .= " DESC";
            $this->mOrderBy[] = $clause;
            return $this;
        }

        private $mGroupBy = [];

        /**
         * @param $col1
         * @param null $col2
         * @return $this
         */
        public function groupBy ($col1, $col2=NULL) {
            if ($col1 === NULL) {
                $this->mGroupBy = [];
                return $this;
            }
            foreach (func_get_args() as $cur)
                $this->mGroupBy[] = $cur;
            return $this;
        }

        private $mHavings = [];

        public function having ($query, $col1=NULL, $col2=NULL) {
            if ($query === NULL) {
                $this->mHavings = [];
                return $this;
            }

            preg_match_all("/(\\?\\?|\\?)/", $query, $matches);
            $args = func_get_args();
            foreach ($matches as $index => $match) {

                if (@$match[$index] == "??" && ! is_array ($args[$index+1]))
                    throw new \InvalidArgumentException("Argument " . ($index+1) . " must be array");
                if (@$match[$index] == "?" && is_array($args[$index+1]))
                    throw new \InvalidArgumentException("Argument " . ($index+1) . " must not be array");
            }

            $this->mHavings[] = func_get_args(); // Includes $query on Index 0
            return $this;
        }


        private $isCachedQuery = false;

        /**
         * Activate Redis Caching (if available) of SELECT statements
         *
         * @param bool $cachedQuery
         * @return $this
         */
        public function cached($cachedQuery=true) {
            $this->isCachedQuery = $cachedQuery;
            return $this;
        }


        private function _buildWhere () {
            if (count ($this->mWheres) == 0)
                return "";
            $elements = [];
            foreach ($this->mWheres as $where) {
                $offset = 0;
                // Group OR Keywords whithin one where() stmt.
                if (preg_match ("/OR/im", $where[0])) {
                    $where[0] = "(" . $where[0] . ")";
                }
                $elements[] = preg_replace_callback("/(\\?\\?|\\?)/",
                    function ($matches) use (&$offset, &$where) {
                        $val = @$where[++$offset];
                        if ($matches[0] == "?") {
                            if ($val instanceof EdbcEntity)
                                    $val = $val->getPrimaryKeyValue();
                            if ($val === NULL) {
                                $val = "NULL";
                            } else {
                                $val = "'" . $this->mCon->escape($val) . "'";
                            }
                            return $val;
                        } else if ($matches[0] == "??") {
                            if ( ! is_array($val))
                                throw new GisException("Parameter must be array for pattern '??'");
                            $tmp = [];
                            foreach ($val as $curVal) {
                                if ($curVal instanceof EdbcEntity)
                                    $curVal = $curVal->getPrimaryKeyValue();
                                if ($curVal === NULL) {
                                    $curVal = "NULL";
                                } else {
                                    $curVal = "'" . $this->mCon->escape($curVal) . "'";
                                }
                                $tmp[] = $curVal;
                            }
                            return implode(",", $tmp);
                        }
                        throw new GisException("Regex Fehler");
                    }, $where[0]

                );
            }
            return "WHERE " . implode (" AND ", $elements);
        }


        /**
         * Escape a string
         *
         * @param $string
         * @return string
         */
        public function escape ($string) {
            return $this->mCon->escape($string);
        }


        private function _buildHaving () {
            if (count ($this->mHavings) == 0)
                return "";
            $elements = [];
            foreach ($this->mHavings as $where) {
                $offset = 0;
                if (preg_match ("/OR/im", $where[0])) {
                    $where[0] = "(" . $where[0] . ")";
                }

                $elements[] = preg_replace_callback("/(\\?\\?|\\?)/",
                    function ($matches) use (&$offset, &$where) {
                        $val = $where[++$offset];
                        if ($matches[0] == "?") {
                            if ($val instanceof EdbcEntity)
                                $val = $val->getPrimaryKeyValue();
                            if ($val === NULL) {
                                $val = "NULL";
                            } else {
                                $val = "'" . $this->mCon->escape($val) . "'";
                            }
                            return $val;
                        } else if ($matches[0] == "??") {
                            if ( ! is_array($val))
                                throw new GisException("Parameter must be array for pattern '??'");
                            $tmp = [];
                            foreach ($val as $curVal) {
                                if ($curVal instanceof EdbcEntity)
                                    $curVal = $curVal->getPrimaryKeyValue();
                                if ($curVal === NULL) {
                                    $curVal = "NULL";
                                } else {
                                    $curVal = "'" . $this->mCon->escape($curVal) . "'";
                                }
                                $tmp[] = $curVal;
                            }
                            return implode(",", $tmp);
                        }
                        throw new GisException("Regex Fehler");
                    }, $where[0]
                );
            }
            return "HAVING " . implode (" AND ", $elements);
        }


        private function _buildSelect () {
            if (count ($this->mSelects) === 0)
                $this->mSelects = ['*'];

            $cached = "";
            if ($this->isCachedQuery)
                $cached = "[CACHED] ";

            $q = "SELECT {$cached}" . implode (", ", $this->mSelects) . " FROM {$this->mTable} ";

            foreach ($this->mJoins as $join) {
                $q .= "\n\t$join ";
            }

            $q .= $this->_buildWhere();


            if (count ($this->mGroupBy) > 0) {
                $q .= " GROUP BY " . implode (", ", $this->mGroupBy);
            }

            if (count ($this->mHavings) > 0) {
                $q .= " " . $this->_buildHaving();
            }

            if (count($this->mOrderBy) > 0) {
                $q .= " ORDER BY " . implode(", ", $this->mOrderBy);
            }



            if ($this->mLimit !== NULL) {
                $q .= " LIMIT {$this->mLimit}";
                if ($this->mOffset !== NULL)
                    $q .= ",{$this->mOffset}";
            }


            return $q;
        }

        private function _buildInsert() {
            $keys = [];
            $vals = [];
            foreach ($this->mSet as $curSet) {
                $keys[] = $curSet[0];
                if ($curSet[1] === NULL) {
                    $vals[] = "NULL";
                } else {
                    $vals[] = "'" . $this->mCon->escape($curSet[1]) . "'";
                }
            }
            $q = "INSERT INTO {$this->mTable} (" . implode (",", $keys) . ") VALUES (" . implode (",", $keys) . ")";
            return $q;
        }

        private function _buildUpdate () {
            $setta = [];
            foreach ($this->mSet as $curSet) {
                $curData = "{$curSet[0]}=";
                if ($curSet[1] === NULL) {
                    $curData = "NULL";
                } else {
                    $curData = "'" . $this->mCon->escape($curSet[1]) . "'";
                }
                $setta[] = $curData;
            }
            $q = "UPDATE {$this->mTable} SET (" . implode (",", $setta) . "){$this->_buildWhere()}";
            return $q;
        }

        private function _buildDelete () {
            $q = "DELETE {$this->mTable}{$this->_buildWhere()}";
            return $q;
        }

        /**
         * @throws DbError
         * @return string
         */
        public function getQuery () {
            switch ($this->mType) {
                case self::T_SELECT:    return $this->_buildSelect();
                case self::T_INSERT:    return $this->_buildInsert();
                case self::T_UPDATE:    return $this->_buildUpdate();
                case self::T_DELETE:    return $this->_buildDelete();

                default: throw new DbError("Invalid QueryType: '{$this->mType}'");
            }
        }

        /**
         * Execute the Query and return ResultSet.
         *
         *
         * @deprecated use getResult() instead
         * @throws DbError
         * @return MySqlResult | LateLoadingResult
         */
        public function e() {
            $result = $this->mCon->query(new RawSql($this->getQuery()));
            if ($this->mPm !== NULL)
                $result = new LateLoadingResult($this->mPm, $result);
            return $result;
        }


        private $lastQueryTime = NULL;

        /**
         * Execute the Query and return ResultSet.
         *
         * @return MySqlResult | LateLoadingResult
         * @throws DbError
         */
        public function getResult() {
            $qTime = microtime(true);
            $result = $this->mCon->query(new RawSql($this->getQuery()));
            $this->lastQueryTime = round (microtime(true) - $qTime, 3);
            if ($this->mPm !== NULL)
                $result = new LateLoadingResult($this->mPm, $result, 0);
            return $result;
        }

        public function getQueryDuration () {
            return $this->lastQueryTime;
        }

        /**
         * @return string
         */
        public function __toString () {
            return $this->getQuery();
        }

    }
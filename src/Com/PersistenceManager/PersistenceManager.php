<?php
/**
 * Created by PhpStorm.
 * User: matthes
 * Date: 04.09.14
 * Time: 16:04
 */


    namespace DbAkl\Com\PersistenceManager;


    use gis\core\exception\GisException;
    use gis\core\exception\MoreThanOneException;
    use gis\core\exception\NoDataException;
    use gis\core\helper\IdGenerator;
    use gis\db\driver\mysql\MySqlConnection;
    use gis\db\driver\mysql\query\InsertQuery;
    use gis\db\driver\mysql\query\UpdateQuery;
    use gis\db\exception\DbError;
    use gis\db\exception\FkNullException;
    use gis\db\opt\fsql\FSql;
    use gis\db\opt\orm\core\Kernel;
    use gis\db\opt\orm\core\ObjectInfo;
    use gis\db\opt\orm\core\PersistenceManagerConfig;
    use gis\db\opt\orm\def\ClassInfo;
    use gis\db\opt\orm\core\OrmConstants;
    use gis\db\opt\orm\exception\InvalidPropertyValueException;
    use gis\db\Sql;

    class PersistenceManager {

        public static $pmIndex = 0;





        /**
         * @var MySqlConnection
         */
        private $mCon;

        /**
         * @var PersistenceManagerConfig
         */
        private $mConfig;

        private $mPmId;


        public function __construct (MySqlConnection $con) {
            self::$pmIndex++;
            Kernel::RegisterPm($this, self::$pmIndex);
            $this->mPmId = self::$pmIndex;
            $this->mCon = $con;
            $this->mConfig = new PersistenceManagerConfig();
        }

        public function getPmId () {
            return $this->mPmId;
        }


        /**
         * Return the Connection
         *
         * @return MySqlConnection
         */
        public function getConnection () {
            return $this->mCon;
        }

        /**
         * Execute a Sql Statment over the Connection
         *
         * @param $stmt
         * @param null $param1
         * @param null $param2
         * @return \gis\db\driver\mysql\MySqlResult
         */
        public function query ($stmt, $param1=NULL, $param2=NULL) {
            $params = func_get_args();
            array_shift($params);
            return $this->getConnection()->query(new Sql($stmt, $params));
        }



        /**
         * @return PersistenceManagerConfig
         */
        public function config() {
            return $this->mConfig;
        }


        /**
         * Return the ClassInfo Object.
         *
         * ClassInfo wraps the ClassDefinition and offers an easy-to-use
         * Interface to retrieve Information about the Entity
         *
         * @param $objOrClassName object|string
         * @throws GisException
         * @return ClassInfo
         */
        public function getClassInfo ($objOrClassName) {
            if (is_object($objOrClassName))
                $objOrClassName = get_class($objOrClassName);
            if ( ! is_string($objOrClassName))
                throw new GisException("getClassInfo() requires parameter 1 to be object or string: " . gettype($objOrClassName) . " found");

            return Kernel::GetClassInfo($objOrClassName);
        }





        /**
         * Baut ein FSql Query für die Aktuelle Tabelle und führ als erstes Select-Feld
         * das Feld <tableName>.<id> ein. (LateLoading Property erwartet die id des Entities in
         * Parameter 1)
         *
         * <tableName>.<id> muss gesetzt sein, damit Joints mit anderen Tabellen funktionieren.
         *
         * @return FSql
         */
        public function fsql($className) {
            if ( ! class_exists($className))
                throw new GisException("Invalid className: $className");
            $classInfo = Kernel::GetClassInfo($className);
            $fsql = new FSql($this->mCon, $this);

            $fsql->from($classInfo->getTableName());
            $fsql->select($classInfo->getTableName() . ".". $classInfo->getPkColName());
            return $fsql;
        }

        /**
         * @param $obj
         * @param $id
         *
         * @return $this
         * @throws GisException
         * @throws \gis\core\exception\MoreThanOneException
         * @throws \gis\core\exception\NoDataException
         */
        public function find ($obj, array $restrictions) {
            if ( ! is_object($obj))
                throw new GisException("Parameter 1 must be valid entity object");
            if (is_integer($id))
                $id = (string)$id;
            if (! is_string($id))
                throw new GisException("Parameter 2 must be valid string. Found: " . var_export($id, TRUE));
            $cInfo = $this->getClassInfo($obj);
            $_tableName = $cInfo->getTableName();
            $_primaryKeyName = $cInfo->getPkPropertyName();


            $selectAttrs = [];
            foreach ($cInfo->getDbPropertyNames() as $prop) {
                if ($cInfo->isLateLoadingProp($prop)) {
                    $obj->$prop = OrmConstants::LATE_LOADING_VALUE;
                    continue;
                }
                $selectAttrs[] = $prop;
            }
            $selectAttrs = implode (",", $selectAttrs);
            try {
                $this->mCon->query(new Sql("SELECT {$selectAttrs} FROM {} WHERE {$_primaryKeyName}=?",
                                           $_tableName,
                                           $id))
                           ->one($obj);
            } catch (NoDataException $ex) {
                throw new NoDataException("No entity found with id '$id'. Table: '{$cInfo->getTableName()}'", 0, $ex);
            } catch (MoreThanOneException $ex) {
                throw new MoreThanOneException("More than one row found on primary Key: '$id' in table: '{$cInfo->getTableName()}'", 0, $ex);
            }
            if (method_exists($obj, "__orm_setOwnerPmId"))
                $obj->__orm_setOwnerPmId($this->getPmId());
            if (method_exists($obj, "enableOrmTrait"))
                $obj->enableOrmTrait();
            if (method_exists($obj, "__orm_getChangedPropertyNames"))
                $obj->__orm_getChangedPropertyNames(TRUE);
            return $obj;
        }


        public function _update ($obj, ObjectInfo $oInfo, ClassInfo $cInfo) {
            $_tableName = $cInfo->getTableName();
            $_pkAttName = $cInfo->getPkColName();

            $query = new UpdateQuery($_tableName);
            $query->setWhere($_pkAttName, $obj->$_pkAttName);

            if (method_exists($obj, "onStore")) {
                $obj->onStore();
            }

            if (method_exists($obj, "__orm_getChangedPropertyNames")) {
                $updateProperties = $obj->__orm_getChangedPropertyNames();
            } else {
                $updateProperties = $cInfo->getDbPropertyNames();
            }


            if (count($updateProperties) === 0) {
                return true;
            }

            $updatedProps = [];
            foreach ($updateProperties as $name) {
                try {
                    $val = $obj->$name;
                } catch (FkNullException $e) {
                    $val = NULL;
                }
                if (is_bool($val)) {
                    $val = $val === TRUE ? "1" : "0";
                }
                if (is_object($val)) {
                    if ( ! $cInfo->isFkProp($name))
                        throw new InvalidPropertyValueException("Object in Property '$name': Objects are only allowed in ForeignKey Properties");
                    if (is_subclass_of($val, $cInfo->getFkClassName($name)))
                        throw new InvalidPropertyValueException("Object in ForeignKey-Property '$name' is expected to be from type '{$cInfo->getFkClassName($name)}; Found " . gettype($val));
                    $curFkObj = ObjectInfo::newInstance($val);
                    $val = $curFkObj->getPkVal();
                }
                $updatedProps[$name] = $val;
                $query->addColumn($name, $val);
            }

            //echo "\n ===> $query";
            $this->mCon->query($query);

            if (method_exists($obj, "onAfterStore")) {
                $obj->onAfterStore($updatedProps);
            }

            if ($this->mCon->affectedRows() !== 1)
                return false;
            return true;
        }


        public function _create ($obj, ObjectInfo $oInfo, ClassInfo $cInfo) {
            $_tableName = $cInfo->getTableName();
            $_pkAttName = $cInfo->getPkPropertyName();

            $insertStatement = new InsertQuery($_tableName);


            $generatedId = NULL;
            if ($this->mConfig->autoCreatePkValue) {
                $generatedId = IdGenerator::GenerateObjectKey();
                $obj->$_pkAttName = $generatedId;
            }

            if (method_exists($obj, "__orm_setOwnerPmId")) {
                if ( ! method_exists($obj, "__orm_getOwnerPmId"))
                    throw new GisException("Object: " . get_class($obj). " is missing __orm_getOwnerPmId() method");
                if ($obj->__orm_getOwnerPmId() === null) {
                    $obj->__orm_setOwnerPmId($this->getPmId());
                }
            }

            if (method_exists($obj, "onStore")) {
                $obj->onStore();
            }

            $updatedProps = [];
            foreach ($cInfo->getDbPropertyNames() as $name) {
                if ($name == $_pkAttName && $generatedId !== NULL) {
                    $insertStatement->addColumn($cInfo->getColNameByPropName($name), $generatedId);
                } else {
                    try {
                        $val = $obj->$name;
                    } catch (FkNullException $e) {
                        $val = NULL;
                    }
                    if (is_bool($val)) {
                        $val = $val === TRUE ? "1" : "0";
                    }
                    if (is_object($val)) {

                        if ( ! $cInfo->isFkProp($name))
                            throw new InvalidPropertyValueException("Object in Property '$name': Objects are only allowed in ForeignKey Properties");
                        if (is_subclass_of($val, $cInfo->getFkClassName($name)))
                            throw new InvalidPropertyValueException("Object in ForeignKey-Property '$name' is expected to be from type '{$cInfo->getFkClassName($name)}; Found " . gettype($val));
                        if ($val === $obj) {
                            // Self-Referencing Object
                            if ($generatedId === NULL)
                                throw new \InvalidArgumentException("Self-Referencing objects are only supported in autoCreatePkValue Mode");
                            $val = $generatedId;
                        } else {
                            $curFkObj = ObjectInfo::newInstance($val);
                            $val = $curFkObj->getPkVal();
                        }
                    }
                    $updatedProps[$cInfo->getColNameByPropName($name)] = $val;
                    $insertStatement->addColumn($cInfo->getColNameByPropName($name), $val);
                }
            }

            $this->mCon->query($insertStatement);

            if ( ! $generatedId && $cInfo->getAutoIncrementPropertyName() !== FALSE) {
                $autoIncPropName = $cInfo->getAutoIncrementPropertyName();
                $value = $this->mCon->getLastInsertId();
                if ($value < 1)
                    throw new GisException("No autoIncrement value available for property '$autoIncPropName'");
                $obj->$autoIncPropName = $value;
            }

            if (method_exists($obj, "onAfterStore")) {
                $obj->onAfterStore($updatedProps);
            }
        }

        public function bindEntity ($obj) {
            if (method_exists($obj, "__orm_setOwnerPmId"))
                $obj->__orm_setOwnerPmId($this->getPmId());
        }


        public function store ($obj) {
            if ( ! is_object($obj))
                throw new GisException("Parameter 1 must be valid entity object");
            if (method_exists($obj, "__orm_getOwnerPmId")) {
                $objPmId = $obj->__orm_getOwnerPmId();
                if ($objPmId !== NULL && $objPmId !== $this->mPmId)
                    throw new DbError("Cannot Store Entity belonging to PersistenceManagerId {$objPmId} on me (PmId: {$this->mPmId})");
            }

            $oInfo = ObjectInfo::newInstance($obj);
            $cInfo = $oInfo->getClassInfo();
            $_pkAttName = $cInfo->getPkPropertyName();

            if (method_exists($obj, "enableOrmTrait"))
                $obj->enableOrmTrait();

            if ($this->config()->autoCreatePkValue) {
                // Normalfall:
                if ($obj->$_pkAttName === NULL) {
                    $this->_create($obj, $oInfo, $cInfo);
                } else {
                    $this->_update($obj, $oInfo, $cInfo);
                }
            } else {
                // Wenn PKs manuell gesetzt werden
                if ( ! $this->_update($obj, $oInfo, $cInfo)) {
                    $this->_create($obj, $oInfo, $cInfo);
                }
            }


            if (method_exists($obj, "__orm_getChangedPropertyNames"))
                $obj->__orm_getChangedPropertyNames(TRUE);

        }

        public function delete ($obj) {
            if ( ! is_object($obj))
                throw new GisException("Parameter 1 must be valid entity object");
            $cInfo = Kernel::GetClassInfo(get_class($obj));
            $_tableName = $cInfo->getTableName();
            $_pkAttName = $cInfo->getPkColName();

            if (method_exists($obj, "onDelete")) {
                $obj->onDelete();
            }

            if ( ! isset ($obj->$_pkAttName))
                throw new GisException("Object is missing primaryKey '{$_pkAttName}'");
            if (empty($obj->$_pkAttName))
                throw new GisException("Invalid PrimaryKey-Value '{$obj->$_pkAttName}' in attribute '$_pkAttName'");
            $this->mCon->query(new Sql("DELETE FROM {} WHERE {$_pkAttName} = ? LIMIT 1", $_tableName, $obj->$_pkAttName));
            unset ($obj);
        }


        public function beginTransaction () {
            $this->mCon->beginTransaction();
        }

        public function commit () {
            $this->mCon->commit();
        }

        public function rollback() {
            $this->mCon->rollback();
        }


    }
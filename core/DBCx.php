<?php

namespace RoSS\DBTools;

/**
 *
 * DBTools : DBCx
 * v0.0.1a
 *
 * Handles database connection and communication.
 * Currently supports PDO + MySQL
 *
 * Status : WIP
 *
 */
class DBCx
{

    private const DBX_CHARSET = 'utf8';
    private $dbx;
    private $schema;
    private $driver;

    function __construct($config)
    {
        if (ConfigFile::checkConfig($config)) {

            // Connection using PDO
            if ($config['connector'] === "pdo") {

                $config = $config['db']['master'];
                $pdo_config = $config['driver'].':host='.$config['server'].';dbname='.$config['schema'].';charset='.self::DBX_CHARSET;
                try {
                    $this->dbx = new \PDO($pdo_config, $config['user'], $config['pwd']);
                } catch(\PDOException $err) {
                    echo "\n Error : PDO : ".$err->getMessage()."\n\n";
                    throw new \Exception("ERROR : PDO Failed");
                }

            }
            else {
                throw new \Exception("ERROR : Connector not supported.");
            }

            $this->driver = $config['driver'];
            $this->schema = $config['schema'];
        }
    }

    /**
     * run
     * Public
     *
     * Executes queries on database
     * and returns results.
     *
     * @param string $sql
     * @param int $pdo_mode
     * @return array|false
     */
    function run($sql="", $pdo_mode = \PDO::FETCH_ASSOC)
    {

        $sql = trim($sql);
        if (!empty($sql)) {
            $data = $this->dbx->query($sql)->fetchAll($pdo_mode);
            return $data;
        }

        return [];
    }

    /**
     * getSchema
     * Public
     *
     * Exposes Database Schema being used by configuration file.
     *
     * @return string
     */
    function getSchema()
    {
        return $this->schema;
    }

}
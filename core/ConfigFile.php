<?php

namespace RoSS\DBTools;

/**
 *
 * DBTools : ConfigFile
 * by RoSS (RodrigoSantosSilva)
 *
 * v0.0.1a
 *
 *
 * Configuration file handler.
 *
 * Configuration file example:
 * JSON text file :
    {
        "connector" : "pdo",
        "db" : {
            "master" : {
                "driver": "mysql",
                "server": "127.0.0.1",
                "schema": "DB_SCHEMA_NAME",
                "user": "dbusername",
                "pwd": "dbpassword"
            }
        }
    }
 *
 * Currently only accepting PDO
 * Status WIP
 *
 */
class ConfigFile
{
    /**
     *
     * get
     * Public
     *
     * Open db configuration file and perform checks
     * to validate format.
     *
     * @param string $fileLocation
     * @return mixed|true
     * @throws \ErrorException
     */
    static function get($fileLocation)
    {
        if (file_exists($fileLocation)) {
            $fileHandler = fopen($fileLocation, "r");
            $configFile = fread($fileHandler, filesize($fileLocation));
            fclose($fileHandler);

            return self::configFileCheck($configFile);

        }
        else {
            throw new \ErrorException("ERROR : Configuration file not found.");
        }
    }

    /**
     * checkConfig
     * Public
     *
     * Expose configFileCheck.
     *
     * @param mixed|json|array $configJson
     * @return boolean|array
     * @throws \ErrorException
     */
    static function checkConfig($configJson)
    {
        return self::configFileCheck($configJson);
    }

    /**
     * configFileCheck
     * Private
     *
     * Verifies a DB configuration file.
     * It can receive a string or an Array. It will check contents
     * format and return , false or a json object.
     *
     * Return is determined by the type of $configFile,
     * if given $configFile is a json string, it returns an array.
     * Case $configFile is array, returns true|false;
     *
     * @param mixed|string|json $configFile
     * @return mixed|true|false|array
     * @throws \ErrorException
     */
    static private function configFileCheck($configFile)
    {
        if (is_string($configFile)) {
            $configFile = trim($configFile);
            $configJson = json_decode($configFile, true);
        }
        else if (is_array($configFile)) {
            $configJson = $configFile;
        }
        else {
            return false;
        }

        if ($configJson !== false) {

            // Configuration file fields
            $configFields = [
                "connector", // pdo
                "db" => [
                    "master" => [
                        "server",
                        "schema",
                        "user",
                        "pwd",
                    ]
                ]
            ];

            if (!self::configFileCheckArray($configFields, $configJson)) {
                throw new \ErrorException("ERROR : Bad configuration JSON ");
            }

            if (is_string($configFile)) {
                return $configJson;
            }
            else { return true; }
        }

        throw new \ErrorException("ERROR : Bad configuration file.");
    }

    /**
     * configFileCheckArray
     * Private
     *
     * Navigate configuration file and check structure.
     *
     * @param array $fieldsToCheck
     * @param array $configJson
     * @return bool
     */
    static private function configFileCheckArray($fieldsToCheck=[], $configJson=false)
    {
        if (empty($configJson) || empty($fieldsToCheck)) { return false; }

        foreach ($fieldsToCheck as $fieldKey => $fieldVal) {

            if (is_string($fieldVal)) {
                if (!isset($configJson[$fieldVal])) {
                    return false;
                }
            }
            else if (is_array($fieldVal)) {
                if (
                    !isset($configJson[$fieldKey])
                    || !self::configFileCheckArray($fieldVal, $configJson[$fieldKey])
                ) {
                    return false;
                }
            }

        }

        return true;
    }
}
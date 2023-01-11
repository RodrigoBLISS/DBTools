<?php

namespace RoSS\DBTools;

/**
 *
 * DBTools : DBMap
 * by RoSS (RodrigoSantosSilva)
 *
 * v0.0.1a
 *
 * A set of tools for DataBase administration.
 * You should be able to MAP a database, its tables and structures
 * and save it as Migration JSON File.
 *
 * This JSON Migration file can compared to other Databases, and the
 * differences can be applied to the database in a variety of ways.
 *
 * Status : WIP
 *
 *
 */
class DBMap
{
    private $config;
    private $dbx;
    private $tbl_list;
    private $tbl_structure=[];
    private $migrationJSON;


    public function __construct($configFile)
    {
        $this->config = ConfigFile::get($configFile);
        $this->dbx = new DBCx($this->config);
    }

    /**
     * getTables
     * Public
     *
     * Retrieves all tables from schema.
     * @return array|false
     */
    public function getTables()
    {

        $this->tbl_list = $this->dbx->run('SHOW TABLES', \PDO::FETCH_COLUMN);
        return $this->tbl_list;

    }


    /**
     * mapTable
     * Public
     *
     * Get table structure.
     *
     * @param $tbl_name
     * @return array|false
     */
    public function mapTable($tbl_name)
    {

        // Table general information
        $tbl_info = $this->normalizeFieldNames(
            $this->getFirst($this->dbx->run('SHOW TABLE STATUS WHERE name = "'.$tbl_name.'"'))
        );
        // If table were found
        if (count($tbl_info) > 0) {

            // Table structure
            $tbl_struct = [];
            foreach ( $this->normalizeFieldNames($this->dbx->run('DESCRIBE '.$tbl_name)) as $fieldInfo) {
                $tbl_struct[$fieldInfo['field']] = $fieldInfo;
            }

            // Table Index Information
            $tbl_index = [];
            foreach ($this->normalizeFieldNames( $this->dbx->run(' SHOW INDEXES FROM '.$tbl_name)) as $indexData) {
                if (!isset($tbl_index[$indexData['key_name']])) { $tbl_index[$indexData['key_name']]=[]; }
                $tbl_index[$indexData['key_name']][intval($indexData['seq_in_index'])-1] = $indexData;
            }

            // Table Triggers
            $tbl_triggers = $this->normalizeFieldNames($this->dbx->run(' SHOW TRIGGERS FROM '.$this->dbx->getSchema().' WHERE `Table` = "'.$tbl_name.'" '));

            // Organize data
            $tbl_schema = [
                "name"      => $tbl_name,
                "engine"    => $tbl_info['engine'],
                "version"   => $tbl_info['version'],
                "create_time"   => $tbl_info['create_time'],
                "row_format"    => $tbl_info['row_format'],
                "collation"     => $tbl_info['collation'],
                "comment"       => $tbl_info['comment'],

                "table"     => $tbl_struct,
                "indexes"   => $tbl_index,
                "triggers"  => $tbl_triggers,

            ];

            // Update data
            $this->tbl_structure[$tbl_name] = $tbl_schema;

            // Return current request
            return $tbl_schema;

        }

        return false;

    }


    /**
     * mapMaster
     * Public
     *
     * Maps every table of configuration file MasterDB,
     * and returns an awesome array
     *
     * @return array
     */
    public function mapMaster()
    {

        $this->log(" Mapping Master : ".$this->dbx->getSchema());
        $this->log("");

        $this->getTables();
        foreach ($this->tbl_list as $tbl) {
            echo ".";
            $this->mapTable($tbl);
            //break;
        }
        $this->log(" Tables mapped : ".count($this->tbl_list)."\n");

        return [
            "schema" => $this->dbx->getSchema(),
            "tables" => $this->tbl_structure
        ];
    }

    /**
     * exportMaster
     * Public
     *
     * Receives a string with a file location and saves
     * a JSON file with the mapping of the configuration
     * master database.
     *
     * @param string $fileName
     * @return true
     * @throws \Exception
     */
    // Export Master map into a json file
    public function exportMaster($fileName)
    {
        $fileName = trim($fileName);
        if (!empty($fileName)) {
            $this->outputJson($this->mapMaster(), $fileName);
            return true;
        }
        throw new \Exception("ERROR : Bad filename");
    }

    // TODO
    // Compare migration data file with master db
    /**
     * compareMaster
     * Public
     *
     * Receives a file location for a migration file exported by
     * DBMap. Compares this structure against the structure of the
     * master database on configuration file.
     *
     * Will return an array containing the diff.
     *
     * @param string $migrationJson_location
     * @return array|void
     * @throws \Exception
     */
    public function compareMaster($migrationJson_location)
    {

        if ($this->loadMigrationJson($migrationJson_location)) {

            $schemaCompareData = [
                "creation" => date("Y-m-d H:i:s"),
                "master" => [
                    "name" => $this->dbx->getSchema(),
                    "diff"      => [],
                    "add"       => [],
                    "remove"    => []
                ],
                "stats" => [
                    "tables" => [
                        "checked" => 0,
                        "changes" => 0
                    ]
                ]
            ];


            // Get all tables from master
            foreach ($this->getTables() as $master_tbl) {
                // table does not exist on migration file, flag for removal
                if (!isset($this->migrationJSON['tables'][$master_tbl])) {

                    $this->log("[-] Remove : ".$master_tbl);
                    $schemaCompareData['master']['remove'][] = $master_tbl;

                }
            }


            // Checking every table from migration file
            foreach ($this->migrationJSON['tables'] as $mig_tbl) {

                $this->log("[i] Checking : ".$mig_tbl['name']);
                $masterdb_tbl = $this->mapTable($mig_tbl['name']);

                if ($masterdb_tbl != false) {
                    $diff = $this->tableDiff($mig_tbl, $masterdb_tbl);
                    if (
                        count($diff['fields']['add']) > 0
                        || count($diff['fields']['change']) > 0
                        || count($diff['fields']['remove']) > 0
                    ) {
                        $schemaCompareData['master']['diff'][$mig_tbl['name']] = $diff;
                        $schemaCompareData['stats']['tables']['changes'] += 1;
                    }
                }
                else {

                    // Table does not exist, flag to add
                    $this->log("[+] Add : ".$mig_tbl['name']);
                    $schemaCompareData['master']['add'][] = $mig_tbl;

                }

                $schemaCompareData['stats']['tables']['checked'] += 1;

            }




            // Returns comparation results
            return $schemaCompareData;

        }
    }

    // Export migration file x master db comparison to json file

    /**
     *
     * compareMasterToJson
     * Public
     *
     * Compares Master DB on configuration file against
     * migration json. Saves a JSON file on location determined
     * by $outputFile.
     *
     * @param string $migrationJson_location
     * @param string $outputFile
     * @return bool
     * @throws \Exception
     */
    public function compareMasterToJson($migrationJson_location, $outputFile)
    {

        $outputFile = trim($outputFile);
        if (!empty($outputFile)) {
            $this->outputJson($this->compareMaster($migrationJson_location), $outputFile);
            return true;
        }

        return false;

    }

    /**
     *
     * tableDiff
     * Private
     *
     * Compare two arrays representing diferent
     * Database table structures.
     *
     * Returns an array with diff.
     *
     * @param array $mig_tbl
     * @param array $master_tbl
     * @return array
     */
    private function tableDiff($mig_tbl, $master_tbl)
    {

        $tbl_diff = [

            "config" => [],
            "fields" => [
                "add"       => [],
                "change"    => [],
                "remove"    => []
            ],
            "index" => [
                "add"       => [],
                "change"    => [],
                "remove"    => []
            ],

            "data" => [
                "migration_file" => $mig_tbl,
                "db"    => $master_tbl
            ],

            "processed" => []
        ];



        // Compare tbl config
        foreach(['engine','collation','comment'] as $f_check) {
            if ($mig_tbl[$f_check] != $master_tbl[$f_check]) {
                $tbl_diff['config'][$f_check] = $mig_tbl[$f_check];
            }
        }



        // Compare structure
        // TODO
        // Transform this into a method
        // Ignore fields like "key" that are changed indirectly
        foreach ($mig_tbl['table'] as $mig_tbl_field => $mig_tbl_field_spec) {

            $tbl_diff['processed'][] = $mig_tbl_field;

            // Check if master has this field
            if (!isset($master_tbl['table'][$mig_tbl_field])) {
                $this->log("[i] Field Add : ".$mig_tbl_field);
                $tbl_diff["fields"]["add"][] = $mig_tbl_field_spec;
            }

            // Check if config is the same
            else {
                foreach($mig_tbl_field_spec as $spec_name => $spec_val) {

                    // Table field config is different
                    if ($master_tbl['table'][$mig_tbl_field][$spec_name] != $spec_val) {

                        if (!isset($tbl_diff["fields"]["change"][$mig_tbl_field])) {
                            $tbl_diff["fields"]["change"][$mig_tbl_field] = [];
                        }
                        $this->log("[!] Field Changes : ".$mig_tbl_field);
                        $tbl_diff["fields"]["change"][$mig_tbl_field][$spec_name] = [
                            "new" => $spec_val,
                            "old" => $master_tbl['table'][$mig_tbl_field][$spec_name]
                        ];

                    }

                }
            }
        }

        // Check if there are removals
        foreach ($master_tbl['table'] as $master_tbl_field) {
            if (!in_array($master_tbl_field['field'], $tbl_diff['processed'])) {
                $this->log("[-] Field Remove : ".$master_tbl_field['field']);
                $tbl_diff["fields"]["remove"][] = $master_tbl_field['field'];
            }
        }



        // Compare Indexes
        // TODO
        // Transform this into a method of itself
        // Add ingore field list like "cardinality"
        foreach ($mig_tbl['indexes'] as $mig_tbl_idx => $mig_tbl_idx_spec) {

            if (!isset($master_tbl['indexes'][$mig_tbl_idx])) {
                $this->log("[i] Index Add : ".$mig_tbl_idx);
                $tbl_diff['index']['add'][] = $mig_tbl_idx_spec;
            }
            else {

                // Get all the column name
                $mig_idx_columns = $this->getIndexColumns($mig_tbl_idx_spec);
                $master_idx_columns = $this->getIndexColumns($master_tbl['indexes'][$mig_tbl_idx]);

                // Check diff between columns
                foreach ($mig_idx_columns as $mig_idx_field => $mig_idx_field_spec) {

                    if(!isset($master_idx_columns[$mig_idx_field])) {
                        $this->log("[!] Index ".$mig_tbl_idx." Add field : ".$mig_idx_field);
                        $tbl_diff['index']['add'][] = $mig_idx_field_spec;
                    }
                    else {
                       foreach ($mig_idx_field_spec as $field => $value) {
                           if ($master_idx_columns[$mig_idx_field][$field]!= $value) {

                               $this->log("[!] Index ".$mig_tbl_idx." Change : ".$field);
                               if(!isset($tbl_diff['index']['change'][$mig_tbl_idx])) {
                                   $tbl_diff['index']['change'][$mig_tbl_idx]=[];
                               }
                               $tbl_diff['index']['change'][$mig_tbl_idx][$field] = [
                                   "new" => $value,
                                   "old" => $master_idx_columns[$mig_idx_field][$field]
                               ];

                           }
                       }
                    }
                }

                // Check for column removals
                foreach (array_keys($master_idx_columns) as $master_idx_colum_name) {
                    if (!isset($mig_idx_columns[$master_idx_colum_name])) {
                        $this->log("[-] Index Remove : ".$mig_tbl_idx." Column : ".$master_idx_colum_name);
                        $tbl_diff['index']['remove'][] = $master_idx_columns[$master_idx_name];
                    }
                }
            }
        }

        // Check index removals
        foreach (array_keys($master_tbl['indexes']) as $master_idx_name) {
            if (!isset($mig_tbl['indexes'][$master_idx_name])) {
                $this->log("[-] Index Remove : ".$master_idx_name);
                $tbl_diff['index']['remove'][] = $master_idx_name;
            }
        }




        // TODO Compare Triggers





        unset($tbl_diff['processed']);
        return $tbl_diff;

    }


    /**
     * normalizeFieldNames
     * Private
     *
     * This method is used to lowcase db fieldnames.
     * To make it easier to manipulate.
     *
     * @param array $array
     * @return array
     */
    private function normalizeFieldNames($array)
    {

        if (is_array($array)) {
            if (count($array) > 0) {
                foreach ($array as $k => $f) {

                    // Navigate inside arrays
                    if (is_array($f)) {
                        $f = $this->normalizeFieldNames($f);
                    }

                    unset($array[$k]);
                    $array[strtolower($k)] = $f;
                }
            }
        }

        return $array;
    }


    /**
     * getFirst
     * Private
     *
     * Returns first item of an array.
     *
     * @param array $array
     * @return array|mixed
     */
    private function getFirst($array)
    {
        if (is_array($array)) {
            if (count($array)>0) {
                return $array[array_keys($array)[0]];
            }
        }
        return [];
    }


    /**
     *
     * outputJson
     * Private
     *
     * Converts an array to a json string and
     * saves it to a file.
     *
     *
     * @param array $data
     * @param string $fileName
     * @return true
     * @throws \Exception
     */
    private function outputJson($data, $fileName)
    {

        $fileData = json_encode($data);
        if ($fileData == false) {
            throw new \Exception("ERROR : Converting data to JSON : ".json_last_error_msg());
        }

        $fjs = fopen($fileName, "a+");
        fwrite($fjs, json_encode($data));
        fclose($fjs);

        return true;
    }


    /**
     *
     * loadMigrationJson
     * Private
     *
     * Load a migration JSON file into
     * an array at property $this->migrationJSON
     *
     * @param string $fileLocation
     * @return true|void
     * @throws \Exception
     */
    private function loadMigrationJson($fileLocation)
    {

        if ( file_exists($fileLocation) ) {

            $fhandler = fopen($fileLocation,"r");
            $fdata = fread($fhandler, filesize($fileLocation));
            fclose($fhandler);

            $this->migrationJSON = json_decode($fdata, true);
            $fdata = '';
            if ($this->migrationJSON == false) {
                throw new \Exception("ERROR : Not able to load migration files : ".json_last_error_msg());
            }
            return true;

        }
    }


    /**
     * getIndexColumns
     * Private
     *
     * Receives a specific array structure and re-organize
     * its keys. Changing the original key to the
     * 'column_name' stored in its value.
     *
     * @param array $idx
     * @return array
     */
    private function getIndexColumns($idx)
    {
        $idxColmns = [];
        foreach ($idx as $idxOrder => $idx_spec) {
            $idxColmns[$idx_spec['column_name']] = array_merge(["order" => $idxOrder],$idx_spec);
        }
        return $idxColmns;
    }

    /**
     * log
     * Private
     * WIP
     *
     * Log function.
     *
     * @param string $msg
     * @return void
     */
    private function log($msg)
    {
        echo "\n".$msg;
    }
}
<?php
use Aws\Sqs\SqsClient;

class SQSSource extends DataSource {
    public $description = "Amazon Simple Queue Service (SQS)";
    public $sqsClient;
    public $config = array();
                
    /**
     * column definition
     *
     * @var array
     */
    public $columns = array(
        'datetime' => array('name' => 'datetime', 'format' => 'Y-m-d H:i:s', 'formatter' => 'date'),
        'timestamp' => array('name' => 'timestamp', 'format' => 'Y-m-d H:i:s', 'formatter' => 'date'),
        'time' => array('name' => 'time', 'format' => 'H:i:s', 'formatter' => 'date'),
        'date' => array('name' => 'date', 'format' => 'Y-m-d', 'formatter' => 'date'),
    );


    public function __construct($config) {
        parent::__construct($config);
        $this->config = $config;

        $awsConfig = array(
            'key' => $config['key'],
            'secret' => $config['secret'],
            'region' => $config['region']
        );

        $this->sqsClient = SqsClient::factory($awsConfig);
    }
    
    public function listSources($data = null) {
        return null;
    }
    
    public function describe($Model) {
        return $Model->_schema;
    }
    
    /**
     * calculate() is for determining how we will count the records and is
     * required to get ``update()`` and ``delete()`` to work.
     *
     * We don't count the records here but return a string to be passed to
     * ``read()`` which will do the actual counting. The easiest way is to just
     * return the string 'COUNT' and check for it in ``read()`` where
     * ``$data['fields'] == 'COUNT'``.
     */
    public function calculate(Model $Model, $func, $params = array()) {
        return 'COUNT';
    }
    
    
    public function create(Model $Model, $fields = array(), $values = array()) {
        $data = array_combine($fields, $values);
        
        return $this->_sendMessage($this->config['sqs']['url'], json_encode($data));
    }
    
    public function update(Model $model, $fields = NULL, $values = NULL, $conditions = NULL) {
        return $this->create($Model, $fields, $values);
    }
    
    public function read(Model $Model, $queryData = array(), $recursive = NULL) {
        $Model->queueURL = $this->config['sqs']['url'];
        $options = array();
        //print_r($queryData); die();
        if ($queryData['fields'] == 'COUNT') {
            return array(array(array('count' => $this->_getQueueSize($Model->queueURL))));
        }
        if (isset($queryData['limit']) && !empty($queryData['limit'])) {
            $options['MaxNumberOfMessages'] = $queryData['limit'];
        }

        $options['QueueUrl'] = $Model->queueURL;
        $response = $this->_receiveMessage($options);
        
        if ($response === false) return array();
        else {

            $results = array();
            if (is_array($response)) {
                foreach($response as $item) {
                    $row[$Model->alias]['body'] = $item['Body'];
                    $row[$Model->alias]['message_id'] = $item['MessageId'];
                    $row[$Model->alias]['receipt_handle'] = $item['ReceiptHandle'];
                    $results[] = $row;
                }
            }
        
            return $results;
        }
    }
    
    public function delete(Model $Model, $conditions = null) {
        $receiptHandle = null;
        
        if (isset($conditions[$Model->alias.'.receipt_handle']) && !empty($conditions[$Model->alias.'.receipt_handle'])) {
            $receiptHandle = $conditions[$Model->alias.'.receipt_handle'];
        }
        elseif (isset($conditions[$Model->alias.'.id']) && !empty($conditions[$Model->alias.'.id'])) {
            $receiptHandle = $conditions[$Model->alias.'.id'];
        }
        else return false;
        
        return $this->_deleteMessage($Model->queueURL, $receiptHandle);
    }

/**
 * Wrappers of AmazonSQS functions
 */    
    protected function _sendMessage($queueURL, $message) {
        if (!is_string($message)) return false;
        
        $this->sqsClient->sendMessage(array(
            'QueueUrl' => $queueURL, 
            'MessageBody' => $message
        ));
        
        return true;
    }
    
    protected function _receiveMessage(array $opt) {
       $response = $this->sqsClient->receiveMessage($opt);
       $messages = $response->get('Messages'); 
        
        if(!empty($messages))
            return $messages;

        return false;
    }
    
    protected function _getQueueSize($queueURL) {
        $sqs = new AmazonSQS();
        return $sqs->get_queue_size($queueURL);
    }
    
    protected function _deleteMessage($queueURL, $receiptHandle, $opt = null) {
        $sqs = new AmazonSQS();
        $response = $sqs->delete_message($queueURL, $receiptHandle, $opt);
                
        if (!$response->isOK()) {
            $this->log($response->body, 'sqs');
            return false;
        }
        
        return true;
    }
}
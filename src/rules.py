rules = {
    # "processor_down_vis.*": {
    #     "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "asvo_consumed_message",
    #     "(.*?),.*Cancel message.*job_id: ([0-9]*) .*": "asvo_cancel",
    #     "(.*?),.*Visibility download complete.*job_id: ([0-9]*) .* product: (.*)": "asvo_complete",
    #     ".*" : "skip"
    # },
    # "processor_conv_queue.*": {
    #    "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*ERROR.*": "skip", 
    #    "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "asvo_consumed_message",
    #    "(.*?),.*Cancel message.*job_id: ([0-9]*) .*": "asvo_cancel",
    #    "(.*?),.*Conversion complete.*job_id: ([0-9]*) .* product: (.*)": "asvo_complete",
    #    ".*" : "skip"        
    # },
    # "proxy\.log.*": {
    #    "(.*?),.*Client: (\d+\.\d+\.\d+\.\d+) Request Complete. Path.*QUERY.*&like=+(.*)%&.*": "obsdownload_query",
    #    ".*" : "skip"        
    # },
    # ".*nglog.*": {
    #     "(.*?),.*Client: (\d+\.\d+\.\d+\.\d+) Request Complete. Path: \/RETRIEVE\?file_id=(\d+_\d+.*)": "ngas_retrieve",
    #     ".*": "skip"
    # },
    "proxy\.log.*": {
        "(.*?),.*Client: (\d+\.\d+\.\d+\.\d+) Request Complete. Path: \/RETRIEVE\?file_id=(\d+_\d+.*)": "ngas_retrieve",
        ".*": "skip"
    }
}  
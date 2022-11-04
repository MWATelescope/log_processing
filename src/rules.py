rules = {
    # "processor_down_vis.*": {
    #     "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*": "skip", # Does not start with a YYYY-MM-DD string
    #     "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "consumed_message",
    #     "(.*?),.*Cancel message.*job_id: ([0-9]*) .*": "cancel",
    #     "(.*?),.*Visibility download complete.*job_id: ([0-9]*) .* product: (.*)": "complete",
    #     ".*" : "skip"
    # },
    # "processor_conv_queue.*": {
    #    "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*": "skip", # Does not start with a YYYY-MM-DD string
    #    "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*ERROR.*": "skip", 
    #    "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "consumed_message",
    #    "(.*?),.*Cancel message.*job_id: ([0-9]*) .*": "cancel",
    #    "(.*?),.*Conversion complete.*job_id: ([0-9]*) .* product: (.*)": "complete",
    #    ".*" : "skip"        
    # },
    "proxy\.log.*": {
       "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*": "skip", # Does not start with a YYYY-MM-DD string
       #".*obs_chunk_download.*": "skip",
       #".*[^\x00-\x7F]+"
       #".*Invalid Command.*": "skip",
       "(.*?),.*Client: (\d+\.\d+\.\d+\.\d+) Path.*QUERY.*&like=+(.*)%&.*": "query",
      # "(.*?),.*Client: (.*) Request.*QUERY.*&like=+(^(?!obs_chunk_download).*)%&.*": "query",
       ".*" : "skip"        
    }

}  
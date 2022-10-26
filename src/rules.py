rules = {
    "processor_down_vis.*": {
        "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*": "skip", # Does not start with a YYYY-MM-DD string
        "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "consumed_message",
        "(.*?),.*Cancel message.*job_id: ([0-9]*) .*": "cancel",
        "(.*?),.*Visibility download complete.*job_id: ([0-9]*) .* product: (.*)": "complete",
        ".*" : "skip"
    }
}
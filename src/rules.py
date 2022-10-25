import handlers

# rules = {
#     "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*": "skip", # Does not start with a YYYY-MM-DD string
#     ".*Starting processor.*": "skip",
#     ".*Connecting to.*" : "skip",
#     ".*Processor exiting.*": "skip",
#     ".*Normal shutdown.*": "skip",
#     ".*never retrieved.*": "skip",
#     ".*Message queued.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "job_queued",
#     ".*Consumed message.*": "skip",
#     ".*Performing conversion.*": "skip",
#     ".*exception occured while running Cotter.*": "skip",
#     ".*error cottering.*": "skip",
#     ".*No files found.*": "skip",
#     #".*": "skip"
# }

# rules = {
#     "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*": "skip", # Does not start with a YYYY-MM-DD string
#     ".*Starting processor.*": "skip",
#     ".*Connecting to.*" : "skip",
#     ".*Message queued.*": "skip",
#     ".*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "consumed_message",
#     ".*Performing download visibility.*": "skip",
#     ".*Cancel message.*job_id: ([0-9]*) .*": "cancel",
#     ".*Processing cancelled: ([0-9]*).*": "cancel",
#     ".*Visibility download complete.*job_id: ([0-9]*) .*": "complete",
#     ".*No files found.*" : "skip", # TODO deal with this
#     ".*b''*" : "skip",
#     ".*No such process.*" : "skip",
#     ".*No such file or directory.*" : "skip",
#     ".*ERROR, $" : "skip",
#     ".*Connect call failed.*" : "skip",
#     ".*referenced before assignment.*" : "skip",
#     ".*has no attribute.*" : "skip",
#     ".*Error getting data.*" : "skip", #deal with
#     ".*still under an embargo.*" : "skip",
#     ".*Observation not found.*" : "skip",
#     ".*Error getting metadata.*" : "skip",
#     ".*Task done.*" : "skip",
#     ".*Staging observation.*" : "skip",
#     ".*file number mismatch.*" : "skip",
#     ".*Cancel message acking.*" : "skip",
#     ".*etting metadata.*" : "skip",
#     ".*etting files.*" : "skip",
#     ".*rchiving files.*" : "skip",
#     ".*Invalid file object.*" : "skip",
#     ".*keyssh.*" : "skip",
#     ".*JobSink.*" : "skip",
#     ".*Normal shutdown.*" : "skip",
#     ".*sha1 hashing.*" : "skip",
#     ".*Error staging data.*" : "skip",
#     ".*Cancelling and requeuing.*" : "skip",
#     ".*Transfer error.*" : "skip",
#     ".*Connection refused.*" : "skip",
#     ".*Could not connect.*": "skip",
#     ".*Socket closed.*": "skip",
#     ".*Disconnected from Rabbit.*": "skip",
#     ".*send frame when closed.*": "skip",
#     ".*Connection is idle.*": "skip",
#     ".*Channel.*": "skip",
#     ".*Closing connection.*": "skip",
#     ".*calling disconnect.*": "skip"
#     #".*.*": "skip"
# }

rules = {
    "^(?![0-9]{4}-[0-9]{2}-[0-9]{2}).*": "skip", # Does not start with a YYYY-MM-DD string
    "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*": "consumed_message",
    "(.*?),.*Cancel message.*job_id: ([0-9]*) .*": "cancel",
    "(.*?),.*Visibility download complete.*job_id: ([0-9]*) .* product: (.*)": "complete",
    ".*" : "skip"
}
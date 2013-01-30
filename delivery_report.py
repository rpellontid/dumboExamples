import sys
import dumbo
from dumbo import main, identityreducer, \
  identitymapper
from dumbo.lib import MultiMapper

def users_parser(key, value):
  tokens = value.split(",")
  k = tokens[0]
  v = ('US', tokens[1])
  yield k, v

def deliveries_parser(key, value):
  tokens = value.split(",")
  k = tokens[0]
  v = ('ST', tokens[1])
  yield k, v

def status_parser(key, value):
  tokens = value.split(",")
  k = tokens[0]
  v = (tokens[1],)
  yield k,v

def reducer1(key, values):
  user = ""
  status = ""
  for v in values:
    if v[0] == 'US':
      user = v[1]
    elif v[0] == 'ST':
      status = v[1]
  # Generate the exit
  if (user != "" and status != ""):
    yield status, (user, key)

def reducer2(key, values):
  status = ""
  info = []
  for v in values:
    if len(v)==2:
      info = v
    elif len(v)==1:
      status = v[0];

  # Generate the exit
  if (status != "" and len(info)>0):
    yield info, status

# Jobs workflow function
def runner(job):
  # Step 1: Prepare users, details deliver
  opts = [ ("inputformat","text"), \
    ("outputformat","text") ]
  multimapper = MultiMapper();
  multimapper.add("users", users_parser)
  multimapper.add("details", deliveries_parser) 
  o1 = job.additer(multimapper, reducer1, \
    opts=opts )

  # Step 2: Get status description
  multimapper = MultiMapper();
  multimapper.add("status", status_parser)
  o2 = job.additer(multimapper, identityreducer, \
    opts=opts, input=[job.root] )

  # Step 3: Join results
  o3 = job.additer(identitymapper, reducer2, \
    opts=opts, input=[o1, o2] )

if __name__ == "__main__":
  from dumbo import main
  dumbo.main(runner)

__author__ = 'jnelson'

from concourse import concourse

db = concourse.Concourse.connect()
record = db.add("name", "Ashleah Nelson", 5)
print(record)
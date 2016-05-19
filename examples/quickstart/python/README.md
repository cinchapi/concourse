# python-quickstart
```bash
$ python quickstart.py
```
This will play a simulation of a Python interpreter session where the quickstart
code is evaluated REPL-style.

```python
# Lets start with some data that describes the employees in a company:
[{'department': 'Engineering',
  'exempt': True,
  'location': 'Atlanta',
  'manager': '@title = Director of Engineering@',
  'name': 'John Doe',
  'role': 'Software Engineer - Backend',
  'salary': 10.0,
  'title': 'Senior Software Engineer'},
 {'department': 'Engineering',
  'exempt': True,
  'location': 'Atlanta',
  'manager_of': '@department = Engineering@',
  'name': 'Jane Doe',
  'role': 'Director',
  'salary': 20.0,
  'title': 'Director of Engineering'},
 {'department': 'Engineering',
  'exempt': False,
  'location': 'Boston',
  'manager': '@title = Director of Engineering@',
  'name': 'Jill Doe',
  'role': 'Software Engineer - Frontend',
  'salary': 10.0,
  'title': 'Software Engineer'},
 {'department': 'Engineering',
  'exempt': True,
  'location': 'Palo Alto',
  'manager': '@title = Director of Engineering@',
  'name': 'Jason Doe',
  'role': 'Quality Engineer',
  'salary': 10.0,
  'title': 'Quality Engineer'},
 {'department': 'Engineering',
  'exempt': True,
  'location': 'Atlanta',
  'manager': '@title = Director of Engineering@',
  'name': 'Adrian Doe',
  'role': 'Software Engineer - Backend',
  'salary': 15.0,
  'title': 'Software Architect'}]

# You can quickly insert the data into Concourse without declaring a schema or creating any structure
>>> Concourse.connect()
>>> records = concourse.insert(data=data)
>>> john, jane, jill, jason, adrian = records

# Now, you can read and modify individual attributes without loading the entire record
# For example, promote Jill and give her the title of Senior Software Engineer. Her current title is:
>>> concourse.get(key='title', record=jill)
Software Engineer

>>> concourse.set(key='title', value='Senior Software Engineer', record=jill)

# After the promotion, Jill's title is:
>>> concourse.get(key='title', record=jill)
Senior Software Engineer

# You can add multiple values to a field
# For example, give Adrian additional responsibilities in the Marketing department
# NOTE: add() appends a new value to a field whereas set() replaces all the values in a field
# NOTE: select() returns ALL the values in a field whereas get() only returns the most recent value
>>> concourse.add('department', 'Marketing', adrian)

# Now, Adrian works in the following departments:
>>> concourse.select(key='department', record=adrian)
['Engineering', 'Marketing']

# You can easily find data that matches a criteria without declaring indexes
# For example, get all the data for all employees that make more than $10
>>> concourse.select(criteria='salary > 10')
{1449069839744008: {'department': ['Engineering'],
                    'exempt': [True],
                    'location': ['Atlanta'],
                    'manager_of': [@1449069839744000,
                                   @1449069839744016,
                                   @1449069839744024,
                                   @1449069839744032],
                    'name': ['Jane Doe'],
                    'role': ['Director'],
                    'salary': [20.0],
                    'title': ['Director of Engineering']},
 1449069839744032: {'department': ['Engineering', 'Marketing'],
                    'exempt': [True],
                    'location': ['Atlanta'],
                    'manager': [@1449069839744008],
                    'name': ['Adrian Doe'],
                    'role': ['Software Engineer - Backend'],
                    'salary': [15.0],
                    'title': ['Software Architect']}}

# Now, get the names of all the Software Engineers in the Atlanta office
>>> concourse.get(key='name', criteria='location = "Atlanta" AND role like "%Software Engineer%"')
{1449069839744000: 'John Doe', 1449069839744032: 'Adrian Doe'}

# You can also view all the values that are stored in a field, across records
# For example, get a list of all job titles
>>> concourse.browse(key='title')
{'Director of Engineering': [1449069839744008],
 'Quality Engineer': [1449069839744024],
 'Senior Software Engineer': [1449069839744000, 1449069839744016],
 'Software Architect': [1449069839744032]}

# Now, get a list of all the names
>>> concourse.browse('name')
{'Adrian Doe': [1449069839744032],
 'Jane Doe': [1449069839744008],
 'Jason Doe': [1449069839744024],
 'Jill Doe': [1449069839744016],
 'John Doe': [1449069839744000]}
# You can analyze how data has changed over time and revert to previous states without downtime
# For example, give Jason a raise and then show how his salary has changed over time
>>> concourse.set('salary', 12.00, jason)
>>> concourse.audit('salary', jason)
{1449069839745027: 'ADD salary AS 10.0 (FLOAT) IN 1449069839744024 AT 1449069839745027',
 1449069888885001: 'REMOVE salary AS 10.0 (FLOAT) IN 1449069839744024 AT 1449069888885001',
 1449069888885002: 'ADD salary AS 12.0 (DOUBLE) IN 1449069839744024 AT 1449069888885002'}

>>> concourse.chronologize('salary', jason)
OrderedDict([(1449069839745027, [10.0]), (1449069888885002, [12.0])])

>>> concourse.diff(key='salary', record=jason, start='10 seconds ago')
{1: [12.0], 2: [10.0]}

# You can use ACID Transactions to make important cross-record changes without the risk of data loss
# For example, change Jill's manager from Jane to John
>>> concourse.stage()
>>> try:
... 	 concourse.unlink('manager_of', jane, jill)
... 	 concourse.link('manager', jill, john)
... 	 concourse.link('manager_of', john, jill)
... except TransactionException:
... 	 concourse.abort()
>>> concourse.select(record=jill)
{'department': ['Engineering'],
 'exempt': [False],
 'location': ['Boston'],
 'manager': [@1449069839744008],
 'name': ['Jill Doe'],
 'role': ['Software Engineer - Frontend'],
 'salary': [10.0],
 'title': ['Senior Software Engineer']}
```

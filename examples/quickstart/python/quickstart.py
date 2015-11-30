#
# The MIT License (MIT)
#
# Copyright (c) 2015 Cinchapi Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from concourse import Concourse, Link, TransactionException

# Let's start with some data that describes employees in a company
data = [
    {
        'name': 'John Doe',
        'department': 'Engineering',
        'title': 'Senior Software Engineer',
        'role': 'Software Engineer - Backend',
        'manager': Link.to_where('title = Director of Engineering'),
        'salary': 10.00,
        'location': 'Atlanta',
        'exempt': True
    },
    {
        'name': 'Jane Doe',
        'department': 'Engineering',
        'title': 'Director of Engineering',
        'role': 'Director',
        'manager_of': Link.to_where('department = Engineering'),
        'salary': 20.00,
        'location': 'Atlanta',
        'exempt': True
    },
    {
        'name': 'Jill Doe',
        'department': 'Engineering',
        'title': 'Software Engineer',
        'role': 'Software Engineer - Frontend',
        'manager': Link.to_where('title = Director of Engineering'),
        'salary': 10.00,
        'location': 'Boston',
        'exempt': False
    },
    {
        'name': 'Jason Doe',
        'department': 'Engineering',
        'title': 'Quality Engineer',
        'role': 'Quality Engineer',
        'manager': Link.to_where('title = Director of Engineering'),
        'salary': 10.00,
        'location': 'Palo Alto',
        'exempt': True
    },
    {
        'name': 'Adrian Doe',
        'department': 'Engineering',
        'title': 'Software Architect',
        'role': 'Software Engineer - Backend',
        'manager': Link.to_where('title = Director of Engineering'),
        'salary': 15.00,
        'location': 'Atlanta',
        'exempt': True
    }

]

concourse = Concourse.connect()

# Quickly insert data without declaring a schema or creating any structure
records = concourse.insert(data=data)
john, jane, jill, jason, adrian = records

# Read and modify individual attributes without loading the entire record
# EXAMPLE: Promote Jill to Senior Software Engineer
print(concourse.get(key='title', record=jill))
concourse.set(key='title', value='Senior Software Engineer', record=jill)

# Add multiple values to a field
# EXAMPLE: Give Adrian additional responsibilities in the Marketing department
# NOTE: add() appends a new value to a field whereas set() replaces all the values in a field
# NOTE: select() returns ALL the values in a field whereas get() only returns the most recent value
concourse.add('department', 'Marketing', adrian)
print(concourse.select(key='department', record=adrian))

# Easily find data that matches a criteria without declaring indexes.
# EXAMPLE: Get the records for all employees that make more than $10
# EXAMPLE: Get the names for all Software Engineers in the Atlanta office
concourse.select(criteria='salary > 10')
print(concourse.get(key='name', criteria='location = "Atlanta" AND role like "%Software Engineer%"'))

# View all the values that are stored in a field, across records
# EXAMPLE: Get a list of all the job titles and then a list of all the names in the company
print(concourse.browse(key='title'))
print(concourse.browse('name'))

# Analyze how data has changed over time and revert to previous states without downtime
# EXAMPLE: Give Jason a raise and then see how is salary has changed over time
concourse.set('salary', 12.00, jason)
print(concourse.audit('salary', jason))
print(concourse.chronologize('salary', jason))
print(concourse.diff(key='salary', record=jason, start='2 seconds ago'))

# You can even query data from the past without doing any extra work
# NOTE: These queries are only for demonstration. They won't return any data because enough time has not passed
concourse.get(key='salary', record=jill, time='two years ago')
concourse.select(key='name', criteria='location = Palo Alto AND department != Engineering in 04/2013')
concourse.browse(keys=['name', 'department'], time='first week of last December')

# ACID Transactions allow you to make important cross-record changes without the risk of data loss
# EXAMPLE: Change Jill's manager from Jane to John
concourse.stage()
try:
    concourse.unlink('manager_of', jane, jill)
    concourse.link('manager', jill, john)
    concourse.link('manager_of', john, jill)
except TransactionException:
    concourse.abort()
print(concourse.select(record=jill))


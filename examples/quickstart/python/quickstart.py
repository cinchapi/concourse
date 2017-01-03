__author__ = 'Jeff Nelson'
#
# The MIT License (MIT)
#
# Copyright (c) 2013-2017 Cinchapi Inc.
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

"""
Simulation of Concourse quickstart as if it were done in a Python REPL session.
"""

from concourse import Concourse, Link, TransactionException
from utils import printd, pprintd
import time

printd('# Lets start with some data that describes the employees in a company:')
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
pprintd(data)
printd('')

printd('# You can quickly insert the data into Concourse without declaring a schema or creating any structure')
printd('>>> Concourse.connect()')
printd('>>> records = concourse.insert(data=data)')
printd('>>> john, jane, jill, jason, adrian = records')
concourse = Concourse.connect(environment='quickstart_'+str(int(time.time())))
records = concourse.insert(data=data)
john, jane, jill, jason, adrian = records
printd('')

printd('# Now, you can read and modify individual attributes without loading the entire record')
printd('# For example, promote Jill and give her the title of Senior Software Engineer. Her current title is:')
printd(">>> concourse.get(key='title', record=jill)")
printd(concourse.get(key='title', record=jill))
printd('')
printd(">>> concourse.set(key='title', value='Senior Software Engineer', record=jill)")
concourse.set(key='title', value='Senior Software Engineer', record=jill)
printd('')
printd("# After the promotion, Jill's title is:")
printd(">>> concourse.get(key='title', record=jill)")
printd(concourse.get(key='title', record=jill))
printd('')

printd('# You can add multiple values to a field')
printd('# For example, give Adrian additional responsibilities in the Marketing department')
printd('# NOTE: add() appends a new value to a field whereas set() replaces all the values in a field')
printd('# NOTE: select() returns ALL the values in a field whereas get() only returns the most recent value')
printd(">>> concourse.add('department', 'Marketing', adrian)")
concourse.add('department', 'Marketing', adrian)
printd('')
printd('# Now, Adrian works in the following departments:')
printd(">>> concourse.select(key='department', record=adrian)")
printd(concourse.select(key='department', record=adrian))
printd('')

printd('# You can easily find data that matches a criteria without declaring indexes')
printd('# For example, get all the data for all employees that make more than $10')
printd(">>> concourse.select(criteria='salary > 10')")
pprintd(concourse.select(criteria='salary > 10'))
printd('')
printd('# Now, get the names of all the Software Engineers in the Atlanta office')
printd(">>> concourse.get(key='name', criteria='location = \"Atlanta\" AND role like \"%Software Engineer%\"')")
pprintd(concourse.get(key='name', criteria='location = "Atlanta" AND role like "%Software Engineer%"'))
printd('')

printd('# You can also view all the values that are stored in a field, across records')
printd('# For example, get a list of all job titles')
printd(">>> concourse.browse(key='title')")
pprintd(concourse.browse(key='title'))
printd('')
printd('# Now, get a list of all the names')
printd(">>> concourse.browse('name')")
pprintd(concourse.browse('name'))

printd('# You can analyze how data has changed over time and revert to previous states without downtime')
printd('# For example, give Jason a raise and then show how his salary has changed over time')
printd(">>> concourse.set('salary', 12.00, jason)")
concourse.set('salary', 12.00, jason)
printd(">>> concourse.audit('salary', jason)")
pprintd(concourse.audit('salary', jason))
printd('')
printd(">>> concourse.chronologize('salary', jason)")
pprintd(concourse.chronologize('salary', jason))
printd('')
printd(">>> concourse.diff(key='salary', record=jason, start='10 seconds ago')")
pprintd(concourse.diff(key='salary', record=jason, start='10 seconds ago'))
printd('')

# You can even query data from the past without doing any extra work
# NOTE: These queries are only for demonstration. They won't return any data because enough time has not passed
concourse.get(key='salary', record=jill, time='two years ago')
concourse.select(key='name', criteria='location = Palo Alto AND department != Engineering in 04/2013')
concourse.browse(keys=['name', 'department'], time='first week of last December')

printd('# You can use ACID Transactions to make important cross-record changes without the risk of data loss')
printd('# For example, change Jill\'s manager from Jane to John')
printd(">>> concourse.stage()", delay=1)
printd(">>> try:")
printd("...   concourse.unlink('manager_of', jane, jill)", delay=1)
printd("...   concourse.link('manager', jill, john)", delay=1)
printd("...   concourse.link('manager_of', john, jill)", delay=1)
printd("... except TransactionException:", delay=1)
printd("...   concourse.abort()", delay=1)
printd('>>> concourse.select(record=jill)', delay=1)
concourse.stage()
try:
    concourse.unlink('manager_of', jane, jill)
    concourse.unlink('manager', jill, jane)
    concourse.link('manager', jill, john)
    concourse.link('manager_of', john, jill)
except TransactionException:
    concourse.abort()
pprintd(concourse.select(record=jill))
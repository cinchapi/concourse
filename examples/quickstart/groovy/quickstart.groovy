/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

@GrabResolver(name='snapshots', root='https://oss.sonatype.org/content/repositories/snapshots/')
@Grab(group='com.cinchapi', module='concourse-driver-java', version='0.5.0-SNAPSHOT', changing=true)

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.TransactionException;

// Let's start with some data that describes employees in a company
def data = [
    [
        'name':'John Doe',
        'department': 'Engineering',
        'title': 'Senior Software Engineer',
        'role': 'Software Engineer - Backend',
        'manager': Link.toWhere('title = Director of Engineering'),
        'salary': 10.00,
        'location': 'Atlanta',
        'exempt': true
    ],
    [
        'name': 'Jane Doe',
        'department': 'Engineering',
        'title': 'Director of Engineering',
        'role': 'Director',
        'manager_of': Link.toWhere('department = Engineering'),
        'salary': 20.00,
        'location': 'Atlanta',
        'exempt': true
    ],
    [
        'name': 'Jill Doe',
        'department': 'Engineering',
        'title': 'Software Engineer',
        'role': 'Software Engineer - Frontend',
        'manager': Link.toWhere('title = Director of Engineering'),
        'salary': 10.00,
        'location': 'Boston',
        'exempt': true
    ],
    [
        'name': 'Jason Doe',
        'department': 'Engineering',
        'title': 'Quality Engineer',
        'role': 'Quality Engineer',
        'manager': Link.toWhere('title = Director of Engineering'),
        'salary': 10.00,
        'location': 'Palo Alto',
        'exempt': true
    ],
    [
        'name': 'Adrian Doe',
        'department': 'Engineering',
        'title': 'Software Architect',
        'role': 'Software Engineer - Backend',
        'manager': Link.toWhere('title = Director of Engineering'),
        'salary': 15.00,
        'location': 'Atlanta',
        'exempt': true
    ]
]

def concourse = Concourse.connect();

// Quickly insert data without declaring a schema or creating any structure
records = concourse.insert(data)
def (john, jane, jill, jason, adrian) = records

// Read and modify individual attributes without loading the entire record
// EXAMPLE: Promote Jill to Senior Software Engineer
println concourse.get('title', jill)
concourse.set('title', 'Senior Software Engineer', jill)

// Add multiple values to a field
// EXAMPLE: Give Adrian additional responsibilities in the Marketing department
// NOTE: add() appends a new value to a field whereas set() replaces all the values in a field
// NOTE: select() returns ALL the values in a field whereas get() only returns the most recent value
concourse.add 'department', 'Marketing', adrian
println concourse.select('department', adrian)

// Easily find data that matches a criteria without declaring indexes.
// EXAMPLE: Get the records for all employees that make more than $10
// EXAMPLE: Get the names for all Software Engineers in the Atlanta office
concourse.select 'salary > 10'
println concourse.get('name', 'location = "Atlanta" AND role like "%Software Engineer%"')

// View all the values that are stored in a field, across records
// EXAMPLE: Get a list of all the job titles and then a list of all the names in the company
println(concourse.browse('title'))
println(concourse.browse('name'))

// Analyze how data has changed over time and revert to previous states without downtime
// EXAMPLE: Give Jason a raise and then see how is salary has changed over time
concourse.set('salary', 12.00, jason)
println(concourse.audit('salary', jason))
println(concourse.chronologize('salary', jason))
println(concourse.diff('salary', jason, Timestamp.fromString('2 seconds ago')))

// You can even query data from the past without doing any extra work
// NOTE: These queries are only for demonstration. They won't return any data because enough time has not passed
concourse.get('salary', jill, Timestamp.fromString('two years ago'))
concourse.select('name', 'location = Palo Alto AND department != Engineering in 04/2013')
concourse.browse(['name', 'department'], Timestamp.fromString('first week of last December'))

// ACID Transactions allow you to make important cross-record changes without the risk of data loss
// EXAMPLE: Change Jill's manager from Jane to John
concourse.stage()
try {
    concourse.unlink('manager_of', jane, jill)
    concourse.link('manager', jill, john)
    concourse.link('manager_of', john, jill)
}
catch(TransactionException e){
    concourse.abort()
}
println concourse.select(jill)

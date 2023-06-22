from cassandra.cluster import Cluster
clstr=Cluster()
session=clstr.connect()
session.execute("drop keyspace if exists mykeyspace;")
session.execute("create keyspace mykeyspace with replication={'class': 'SimpleStrategy', 'replication_factor' : 2};")

session=clstr.connect('mykeyspace')
qry= '''
create table movies (
   title text,
   emptySeats set<int>,
   time timestamp,
   primary key(title)
);'''
session.execute(qry)

qry= '''
create table reservations (
   reservationID uuid,
   bookingTime timestamp,
   movieTitle text,
   seatNumber int,
   username text,
   primary key(reservationID)
);'''
session.execute(qry)

session.execute("insert into movies (title, emptySeats, time) values ('abc', {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}, '2023-07-01 18:00');")
session.execute("insert into movies (title, emptySeats, time) values ('def', {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25}, '2023-07-01 13:00');")
session.execute("insert into movies (title, emptySeats, time) values ('ghj', {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}, '2023-07-05 15:00');")

session.execute("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'abc', 8, 'AdamC');")
session.execute("update movies set emptySeats = emptySeats - {8} where title = 'abc';")
session.execute("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'abc', 9, 'MaciejW');")
session.execute("update movies set emptySeats = emptySeats - {9} where title = 'abc';")


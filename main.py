from cassandra.cluster import Cluster
import uuid
import random
import string
clstr = Cluster()
session = clstr.connect('mykeyspace')





def generate_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

def large_data(session):
    random_strings = []
    for _ in range(100):
        string_length = random.randint(5, 15)
        random_string = generate_random_string(string_length)
        random_strings.append(random_string)

    for movie in random_strings:
        load = session.prepare("insert into movies (title, emptySeats, time) values (?, {1, 2, 3, 4, 5}, '2023-07-02 18:00');")
        session.execute(load.bind([movie]))

def stress_test_one(session):
    test = True
    session.execute("insert into movies (title, emptySeats, time) values ('movieOne', {1, 2, 3}, '2023-08-01 18:00')")
    session.execute("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'movieOne', 1, 'AdamC')")
    try:
        session.execute("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'movieOne', 1, 'MaciejW')")
    except:
        test = False
    assert test == True

def stress_test_two(session):
    pass

def stress_test_three(session):
    test = True
    session.execute("insert into movies (title, emptySeats, time) values ('movieThree', {1, 2, 3}, '2023-08-03 18:00')")
    seats = {1, 2, 3}
    for seat in seats:
        test2 = session.prepare("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'movieThree', ?, 'AdamC')")
        session.execute(test2.bind([seat]))
    test3 = session.prepare("update movies set emptySeats = emptySeats - ? where title = 'movieThree'")
    session.execute(test3.bind([seats]))
    try:
        session.execute("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'movieThree', 1, 'MaciejW')")
    except:
        test = False

    assert test == True

def stress_test_four(session):
    test = True
    session.execute("insert into movies (title, emptySeats, time) values ('movieFour', {1, 2, 3}, '2023-08-04 18:00')")
    for i in range(50):
        try:
            session.execute("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'movieFour', 1, 'MaciejW')")
            session.execute("update movies set emptySeats = emptySeats - {1} where title = 'movieFour'")
            session.execute("delete from reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), 'movieFour', 1, 'MaciejW')")
            session.execute("update movies set emptySeats = emptySeats + {1} where title = 'movieFour'")
        except:
            test = False
            break
    
    assert test == False











def print_movies(session):
    films = session.execute("select * from movies;")
    for film in films:
        print('- title:{} emptySeats:{} time:{}'.format(film[0],film[1],film[2]))

def print_reservations(session):
    reservations = session.execute("select * from reservations;")
    for reservation in reservations:
        print('- reservationID:{} bookingTime:{} movieTitle:{} seatNumber:{} username:{} '.format(reservation[0],reservation[1],reservation[2],reservation[3],reservation[4]))

def make_reservation(session):
    print("films to choose:")
    print_movies(session)
    print("enter reservation details")
    title = input("Title of the film: ")
    seat = int(input("Choose one of free seats: "))
    username = input("Your username: ")

    try:
        films = session.execute("select * from movies;")
        for film in films:
            if film[0]==title:
                free_seats = film[1]
        if(seat in free_seats):
            update = session.prepare("insert into reservations (reservationID, bookingTime, movieTitle, seatNumber, username) values (uuid(), currentTimestamp(), ?, ?, ?);")
            session.execute(update.bind([title, seat, username]))
            movie_update = session.prepare("update movies set emptySeats = emptySeats - ? where title = ?;")
            session.execute(movie_update.bind([{seat}, title]))
        else:
            print("that seat is already taken reservation was not accepted")
    except(Exception):
        print("wrong input, reservation was not accepted")

def cancel_reservation(session):
    id = (input("Give reservation id: "))
    id = uuid.UUID(id)
    try:
        check_id = session.prepare("select * from reservations where reservationID = ?;")
        check = session.execute(check_id.bind([id]))
        if check:
            reservations = session.execute("select * from reservations;")
            for reservation in reservations:
                if id == reservation[0]:
                    title = reservation[2]
                    seat = reservation[3]
            empty_seat = session.prepare("update movies set emptySeats = emptySeats + ? where title = ?;")
            session.execute(empty_seat.bind([{seat}, title]))
            cancel_id = session.prepare("delete from reservations where reservationID = ?;")
            session.execute(cancel_id.bind([id]))
            print("you cancelled your reservation")
        else:
            print("no such reservation has been made")
    except Exception:
        print("wrong format")

def update_reservation(session):
    #try:
        id = input("id of reservation to be updated: ")
        id = uuid.UUID(id)
        reservations = session.execute("select * from reservations;")
        for i in reservations:
            if i[0] == id:
                print("give target values of the update (leave empty if no change desired)")
                seat = input("new seat: ")
                username = input("new username: ")
                if seat != "":
                    seat = int(seat)
                    films = session.execute("select * from movies;")
                    for film in films:
                        if film[0] == i[2]:
                            if seat in film[1]:
                                empty_seat = session.prepare("update movies set emptySeats = emptySeats + ? where title = ?;")
                                session.execute(empty_seat.bind([{i[3]}, film[0]]))
                                movie_update = session.prepare("update movies set emptySeats = emptySeats - ? where title = ?;")
                                session.execute(movie_update.bind([{seat}, film[0]]))
                                seat_change = session.prepare("update reservations set seatNumber = ? where reservationID = ?;")
                                session.execute(seat_change.bind([seat, id]))
                if username != "":
                    username_change = session.prepare("update reservations set username = ? where reservationID = ?;")
                    session.execute(username_change.bind([username, id]))
        else:
            print("no reservation with that id")
    #except Exception:
        #print("this update is unsiutable")

#application

while True:
    print("What would you like to do?")
    print("Choose by putting a number and confirm it with ENTER")
    print("1: show all movies")
    print("2: show all reservations")
    print("3: make reservation")
    print("4: cancel reservation")
    print("5: update your reservation")

    action = input("your choice: ")

    if action == "1":
        print_movies(session)
    elif action == "2":
        print_reservations(session)
    elif action == "3":
        make_reservation(session)
    elif action == "4":
        cancel_reservation(session)
    elif action == "5":
        update_reservation(session)
    elif action == "6":
        stress_test_one(session)
    elif action == "7":
        stress_test_three(session)
    elif action == "8":
        stress_test_four(session)
    elif action == "9":
        large_data(session)
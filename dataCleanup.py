import sqlite3 as sql

def dropBadStates():
    """This function goes into the database and drops any rows that have no state or give the full state name instead
        of its two letter abbreviation. Another lookup table could be used to convert names to abbreviations but the
        count (1k) of entries fitting this is relatively low. Use of lookup table would be advised for more accurate
        results. Should be ran first.
        NOTE: Can be edited to convert the state name to its abbreviation using the uszips table if more accuracy is
        desired."""
    conn = sql.connect('MobileHomes.db')
    with conn:
        conn.execute('''DELETE FROM mobile_homes WHERE length(state) > 2 or state is null;''')
    conn.close()

def dropBadZips():
    """Some of the rows in the database provide only a state as the location. While this would be adequate for
        state level analysis, I'm doing it on the zipcode/town level so these entries are meaningless for my purposes
        since no income can be determined for the town it resides in. If you wish to run state level analysis,
        I'd recommend leaving these as it drops about 1k rows."""
    conn = sql.connect('MobileHomes.db')
    with conn:
        conn.execute('''DELETE FROM mobile_homes WHERE city is null and zip is null;''')
    conn.close()

def convertLots():
    """This function will address the issue of the lots data being in the form of strings by removing 'lots'
        from the entries and converting the string of digits to an integer. Must be run prior to fillNullLots()
        as that function assumes the entries are integers.
        NOTE: Should alter web scraper to only take the number of lots as an Integer to avoid this step for future uses."""
    conn = sql.connect('MobileHomes.db')
    curs = conn.cursor()
    curs2 = conn.cursor()
    curs.execute('''SELECT rowid, lots FROM mobile_homes WHERE lots IS NOT null;''')
    rowLotList = curs.fetchall()
    curs.execute('''ALTER TABLE mobile_homes RENAME TO mobile_homes_old;''')
    curs.execute('''CREATE TABLE mobile_homes (name TEXT, address TEXT, zip INTEGER, state TEXT, city TEXT, lots INTEGER, seniors INTEGER);''')
    curs.execute('''SELECT * FROM mobile_homes_old;''')
    for row in curs:
        if row[4] is not None:
            lotInt = int(row[4].split(' ')[0])
        else:
            lotInt = None
        curs2.execute('''INSERT INTO mobile_homes VALUES (?,?,?,?,?,?,?);''',
                        (row[0], row[1], row[2], row[3], lotInt, row[5], row[6]))
    conn.commit()
    conn.close()

def findZips():
    """This function will use the zipcode table to replace any null or malformed zipcode values given by the site when it
        was scraped. Can only be run after locations with null zip and city have been removed."""
    conn = sql.connect('MobileHomes.db')
    curs1 = conn.cursor()
    curs2 = conn.cursor()
    curs1.execute('''SELECT rowid, city, state FROM mobile_homes WHERE zip IS null OR NOT length(zip) = 5;''')
    for row in curs1:
        rowid = row[0]
        city = row[1]
        state = row[2]
        curs2.execute('''SELECT zip FROM uszips WHERE city = ? AND state_id = ?;''', (city, state))
        zipcode = int(curs2.fetchone()[0])
        curs2.execute('''UPDATE mobile_homes SET zip = ? WHERE rowid = ?''', (zipcode, rowid))
    conn.commit()
    conn.close()

def fillNullLots():
    """This function takes the median number of lots for a state and assigns that to whatever locations in said state
        had no number of lots listed. Must be called after the convertLots() function if that function is needed. If lots
        are already stored in integer form, this can be run without calling said function."""
    conn = sql.connect('MobileHomes.db')
    curs1 = conn.cursor()
    curs2 = conn.cursor()
    stateMedians = {}
    curs1.execute('''SELECT rowid, state FROM mobile_homes WHERE lots IS null;''')
    for row in curs1:
        if row[1] in stateMedians.keys():
            median = stateMedians[row[1]]
        else:
            curs2.execute('''SELECT lots FROM mobile_homes WHERE state = ? AND lots IS NOT null ORDER BY lots LIMIT 1
                            OFFSET (SELECT COUNT(*) FROM mobile_homes WHERE state = ? AND lots IS NOT null) / 2;''', (row[1], row[1]))
            median = curs2.fetchone()[0]
            stateMedians[row[1]] = median
        curs2.execute('''UPDATE mobile_homes SET lots = ? WHERE rowid = ?;''', (median, row[0]))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    dropBadStates()
    dropBadZips()
    convertLots()
    findZips()
    fillNullLots()
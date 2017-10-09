import sys

## Defining variable for print messages
customPrint = sys.stdout.write

# Function for printing message on console window
def consoleprint(msgVal):
    try:
        customPrint("\n"+msgVal)
    except:
        customPrint("")

## Function for adjusting Generation, Consumption values
def adjust_Gen_Con(passedVal):
    adjustedVal = passedVal
    try:
        if passedVal == "n/e":
            adjustedVal = 0
        elif passedVal == "N/A ":
            adjustedVal = 0
        elif passedVal == "N/A":
            adjustedVal = 0
        elif passedVal == "-":
            adjustedVal = 0
    except:
        return 0
    return adjustedVal

def date2str(dt, deliminter, order=0):
    dateStr = ''
    dt = str(dt).split(' ')[0].split('-')
    if order == 0:
        dateStr = dt[0] + deliminter + dt[1] + deliminter + dt[2]
    else:
        dateStr = dt[2] + deliminter + dt[1] + deliminter + dt[0]

    return dateStr


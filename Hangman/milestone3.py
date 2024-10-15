Word_List = ["apple", "pineapple", "banana", "kiwi", "mango"]
word = ""
def check_guess(guess):
    while True:
        if guess in word:
            print(f"Good guess{guess} in {word}")
        else:
            print(f"Sorry, {guess} is not in the word. Try again." )

def ask_for_input():
    guess = str(input("Guess a letter: "))
    if len(guess) == 1:
        print("Good Guess")
        return True
    else:
        print("Oops! That is not a valid input.")
        return False
    
    check_guess()
ask_for_input()
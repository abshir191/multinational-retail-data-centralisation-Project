import random

Word_List = ["apple", "pineapple", "banana", "kiwi", "mango"]
word = random.choice(Word_List).lower()

def check_guess(guess):
    guess = guess.lower()
    if guess in word:
        print(f"Good guess{guess} in {word}")
    else:
        print(f"Sorry, {guess} is not in the word. Try again." )

def ask_for_input():
    while True:
        guess = input("guess a letter").strip()
        if guess.isalpha() and len(guess) == 1:
            print("Good Guess")
            check_guess(guess)
            break
        else:
            print("Invalid letter. Please, enter a single alphabetical character.")
ask_for_input()

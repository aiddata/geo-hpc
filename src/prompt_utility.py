
import sys


class PromptKit():
    """Functions for generating user prompts.

    Attributes:

        interface (bool): whether to use interface (true) 
                          or automate prompts (false)

    """
    def __init__(self):

        self.interface = False


    def user_prompt_bool(self, question):
        """Prompt user for boolean response to question.

        Args:
            question (str): question to present to user
        Returns:
            (bool): response to prompt
        """
        valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
        
        while True:
            sys.stdout.write(str(question) + " [y/n] \n> ")
            choice = raw_input().lower()

            if choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


    def user_prompt_use_input(self, conditional=None, statement=0, value=None):
        """Prompt user with option to use entered/existing input.

        Args:
            conditional (bool): whether or not an external, user defined condition has been met
            statement (str): statement / question to pass to promp_bool if conditional bool is not given
            value (): existing value the user is being asked to confirm
        Returns:
            bool
        Exits:
            If neither a "value" or "statement" arg are provided
            User decides not to use input and also decides not to provide a replacement 
        """
        if not statement:
            if value == None:
               quit("Error - No statement or value given to user_prompt_use_input.")

            statement = "Use the following? : \"" + str(value) + "\""

        if conditional == None:
            conditional = self.user_prompt_bool(str(statement))

        if not conditional:
            redo = self.user_prompt_bool("Use a new input [y] or exit [n]?")

            if not redo:
               quit("No input given at open prompt.")

            return False

        return True


    def user_prompt_open(self, question, check, new_val=(0,0) ):
        """Prompt user for open ended response and validate.

        Args:
            question (str): question presented to user at prompt
            check (fun): function to validate input provided by user
            new_val (Tuple(bool, bool)): first item indicates whether a value is required (none exists)
                                         if first item is false, second item is the existing value
        Returns:
            validated user input
        Exits:
            interface is activve and input/existing val is invalid and user elects not to provide new input
            interface is not active and input/existing val is invalid
        """
        while True:

            print new_val
            if not new_val[0]:
                raw_answer = new_val[1] 
                use_answer = True
            else:
                sys.stdout.write(question + " \n> ")
                raw_answer = raw_input()
                use_answer = self.user_prompt_use_input(value=raw_answer)

            if use_answer:
                valid, checked_val, error = check(raw_answer)
                
                if not valid:

                    if error != None:
                        error = "("+error+")"
                    else:
                        error = ""

                    if self.interface and not self.user_prompt_bool("Invalid input " + error + "\nUse a new answer [y] or exit [n]?"):
                        quit("No answer given at open prompt.")
                    elif not self.interface:
                        quit("Invalid automated input.")
  
                else:
                    return checked_val



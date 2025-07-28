import tkinter as tk

def click(event):
    global expression
    text = event.widget.cget("text")
    if text == "=":
        try:
            result = str(eval(expression))
            screen_var.set(result)
            expression = result
        except Exception as e:
            screen_var.set("Error")
            expression = ""
    elif text == "C":
        expression = ""
        screen_var.set("")
    else:
        expression += text
        screen_var.set(expression)

# Main window
root = tk.Tk()
root.title("Calculator")
root.geometry("350x500")
root.configure(bg="#222")

expression = ""
screen_var = tk.StringVar()

# Screen
screen = tk.Entry(root, textvar=screen_var, font="Arial 20 bold", bd=10, relief=tk.RIDGE, justify="right")
screen.pack(fill=tk.BOTH, ipadx=8, ipady=8, pady=10)

# Buttons frame
button_frame = tk.Frame(root, bg="#222")
button_frame.pack()

buttons = [
    "7","8","9","/",
    "4","5","6","*",
    "1","2","3","-",
    "0",".","C","+",
    "="
]

row = 0
col = 0
for b in buttons:
    btn = tk.Button(button_frame, text=b, font="Arial 18 bold", width=5, height=2, relief=tk.GROOVE)
    btn.grid(row=row, column=col, padx=5, pady=5)
    btn.bind("<Button-1>", click)
    col += 1
    if col > 3:
        col = 0
        row += 1

root.mainloop()

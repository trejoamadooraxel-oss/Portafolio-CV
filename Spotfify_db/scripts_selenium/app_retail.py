'''
--------------------------------------------------------------------------

ENVIRONMENT: GLOBAL
PORTAL: RETAIL LINK 
VERSION: V5
CREATION DATE: 2025/06/25
CHAIN: 00009
DEVELOPER: AXEL TREJO AMADOR
REVIEWED BY: AXEL TREJO AMADOR
CREATION DATE: 2025/06/22 00:00 pm
LAST UPDATE: 2025/07/16 09:00 am
UPDATE: 'LUGAR O APARTADO DONDE SE REALIZO EL AJUSTE'
--------------------------------------------------------------------------

OBSERVACIONES.
    Applicacion para validar los token de acceso a Retail Link "Walmart"
    con mapeo de excepciones como token vencido, token invalido, cambio 
    de pass, o campos a llenar

--------------------------------------------------------------------------
'''

#Importamos tkinter
import tkinter as tk
from tkinter import messagebox, scrolledtext


#Se crea la ventana princiapal
windows = tk.Tk()
windows.config(background='black')

#titulo de la ventana
windows.title('Real Metrics - Verificador y Acceso a portal Retail Link V1.0')

#Dimensions para la ventana
windows.geometry("650x330")

#Estructura y Acciones

code = 201
bg = 'PaleTurquoise1'
fg = 'snow'
bd = 10
cursor = "hand2"
font_tachado = ("Roboto",10,"overstrike")
font_normal = ("Roboto",10,"normal")
font_negrita = ("Roboto",10,"bold")
font_cursiva = ("Roboto",10,"italic")
font_subrayado = ("Roboto",10,"underline")


#Funciones y eventos:
def obtener_text():
    #for credencial in credenciales:
    text_user = user_input.get()
    if text_user.strip() == "":
        messagebox.showerror("Error", "El User no puede estar vacío.")
        bandera_user = 0
    else:
        bandera_user = 1
        #messagebox.showinfo("OK", f"Texto ingresado: '{text_user}'")

    text_pass = password_input.get()
    if text_pass.strip() == "":
        messagebox.showerror("Error", "El Password no puede estar vacío.")
        bandera_pass = 0
    else:
        bandera_pass = 1
        #messagebox.showinfo("OK", f"Texto ingresado: '{text_pass}'")

    text_token = token_input.get()
    if text_token.strip() == "":
        messagebox.showerror("Error", "El Token no puede estar vacío.")
        bandera_token = 0
    else:
        bandera_token = 1
        #messagebox.showinfo("OK", f"Texto ingresado: '{text_token}'")

    if bandera_pass and bandera_user and bandera_token == 1:
        textbox.delete("1.0", tk.END)
        textbox.insert(tk.END,f"User: {text_user} \nPassword: {text_pass} \nToken: {text_token}\n", "negro")
        textbox.insert(tk.END, f"Verificacion: ","negro")
        if code == 200:
            textbox.insert(tk.END, f"SUCCESSFUL\n", "verde")
        if code != 200:
            textbox.insert(tk.END, f"ERROR: PASSWORD VENCIDO\n", "red")
        check_successful()


    else:
        textbox.insert(tk.END, f"No has igresado los campos necesarios:")
    """print(f'User: {text_user}')
    print(f'Password: {text_pass}')
    print(f'Token: {text_token}')

    """

def check_entries(*args):
    """Habilita el botón solo si los tres Entry tienen texto."""
    if user_input.get().strip() and password_input.get().strip() and token_input.get().strip():
        button_verification.config(state="normal", bg = 'dodger blue', fg = 'white')
    else:
        button_verification.config(state="disabled")

def check_successful(event=None):
    contenido = textbox.get("1.0", tk.END)
    #print(contenido)# Obtener todo el contenido
    if "successful" in contenido.lower():
        # Comparación case-insensitive
        button_session.config(state="normal", bg = 'green', fg = 'white')
    else:
        button_session.config(state="disabled", bg = 'red', fg = 'white')

#Creamos la ventana del marco
marco_1 = tk.Frame(windows,)
marco_1.config(bg = 'snow', width=200, height=200,)
marco_1.columnconfigure(0, weight=1, minsize=10)
marco_1.columnconfigure(1, weight=1, minsize=10)
marco_1.columnconfigure(2, weight=1, minsize=10)

#Creamos una nueva ventana dentro de la columna
frame_botones = tk.Frame(marco_1)
frame_botones.config(bg = 'snow', width=200, height=200,)

user_var = tk.StringVar()
password_var = tk.StringVar()
token_var = tk.StringVar()

user_var.trace_add("write", check_entries)
password_var.trace_add("write", check_entries)
token_var.trace_add("write", check_entries)
#text obtener texto
user_etiqueta = tk.Label(marco_1,
                         text="User:",
                         bg = 'snow',
                         font=font_normal)
user_input = tk.Entry(marco_1,
                   relief="groove",
                   textvariable=user_var,
                   width=30
                   )
password_etiqueta = tk.Label(marco_1,
                         text="Password:",
                         bg = 'snow',
                         font=font_normal)
password_input = tk.Entry(marco_1,
                   relief="groove",
                   textvariable=password_var,
                   width=30
                   )
token_etiqueta = tk.Label(marco_1,
                         text="Token:",
                         bg = 'snow',
                         font=font_normal)

token_input = tk.Entry(marco_1,
                   relief="groove",
                   textvariable=token_var,
                   width=60
                   )

#Caja consola

textbox = scrolledtext.ScrolledText(marco_1, width=40, height=10)
textbox.tag_configure("verde", foreground="green")
textbox.tag_configure("negro", foreground="black")
textbox.tag_configure("red", foreground="red")
textbox.bind("<KeyRelease>", check_successful)



#Botones
button_verification = tk.Button(frame_botones,
                    text="Valida \nCredenciales",
                    cursor=cursor,
                    font=font_negrita,
                    bg = 'snow',
                    #activeforeground="white",
                    #activebackground="blue",
                    relief="groove",
                    state="disabled",
                    #disabledforeground="light blue",
                    command=obtener_text
                    )

button_session = tk.Button(frame_botones,
                    text="Iniciar \nSession",
                    cursor=cursor,
                    bg = 'snow',
                    font=font_negrita,
                    #activeforeground="white",
                    #activebackground="blue",
                    relief="groove",
                    state="disabled",
                    #disabledforeground="light blue",
                    #command=check_successful
                    )
#Conjunto de Packets

marco_1.grid(row=0, column=0, padx=10, pady=10)
user_etiqueta.grid(row = 0, column = 0, padx=5, pady=5, sticky="we")
user_input.grid(row = 0, column = 1, padx=5, pady=5, sticky="we")
password_etiqueta.grid(row = 1, column = 0, padx=5, pady=5, sticky="we")
password_input.grid(row = 1, column = 1, padx=5, pady=5, sticky="we")
token_etiqueta.grid(row = 2, column = 0, padx=5, pady=5, sticky="we")
token_input.grid(row = 2, column = 1, padx=5, pady=5, sticky="we")

textbox.grid(row = 5, column = 1, padx=5, pady=10, sticky="we")

frame_botones.grid(row=5, column=0, sticky="we")
button_verification.grid(row = 1, column = 0, padx=5, pady=5,sticky="we")
button_session.grid(row = 2, column = 0, padx=5, pady=5,sticky="we")
#LOOP
windows.mainloop()

def null_py(var, val):
  if var is None:
    return val
  else:  
    return var

def none_to_null(I_input):
    if I_input == None:
        return I_input
    for i in range(len(I_input)):
        if I_input[i] == None:

            I_input[i] = 'NULL'
    I_input = tuple(I_input)
    return I_input
    
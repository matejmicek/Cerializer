cpdef serialize(int a, char b):
    serialized = open('serialized', 'wb')
    cdef char n[4]
    n[0] = a
    n[1] = b
    serialized.write(n)
    serialized.close()

    input_file = open('serialized', 'rb')
    cdef char p[4]
    print(input_file.read(1))

    print(p[0])
    print(p[1])

class Header:
    """Essa classe representa o Header das mensagens.

    Ela é responsável por armazenar o formato dos headers e realizar a conversão deles.
    """

    def __init__(self, tipo_primitiva : int, addr_rem : str, addr_dest: str, tam : int):
        """Construtor da classe Header.

        Recebe os parâmetros e armazena eles nos atributos.

        Args:
            tipo_primitiva (int): O tipo da primitiva que será enviada.
            addr_rem (str): Identificador do remetente.
            addr_dest (str): Identificador do destinatário.
            tam (int): Tamanho do cabeçalho.
        """
        self.tipo_primitiva = tipo_primitiva
        self.id_rem = f"{addr_rem[0]}:{addr_rem[1]}"
        self.id_dest = f"{addr_dest[0]}:{addr_dest[1]}"
        self.tam = tam

    def __str__(self):
        """Método Dunder para formatar a impressão do objeto.

        Returns:
            str: Uma string que será impressa.
        """
        return f"{self.tipo_primitiva}\n{self.id_rem}\n{self.id_dest}\n{self.tam}"
    
    def to_bytes(self) -> bytes:
        """Método para converter o objeto Header em bytes.
        
        Converte o objeto e seus atributos para bytes, usando o separador |.

        Returns:
            bytes: Atributos do objeto separados por | em bytes.
        """
        return f"{str(self.tipo_primitiva)}|{self.id_rem}|{self.id_dest}|{str(self.tam)}".encode("utf-8")
    
    def to_string(self) -> str:
        """Método para converter o objeto Header em string.
        
        Converte o objeto e seus atributos para string, usando o separador |.

        Returns:
            string: Atributos do objeto separados por | em string.
        """
        return f"{str(self.tipo_primitiva)}|{self.id_rem}|{self.id_dest}|{str(self.tam)}"
    
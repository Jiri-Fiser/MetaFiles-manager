alphabet = "abcdefghij-lmnopqrstuvwxyzABCDEFGHIJ.LMNOP_RSTUVWXYZ0123456789"
second_alphabet = """ !"#$%&'()*+,kK/:;<=>?@[\]^Q`{|}~ ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿĀāĂăĄąĆćĈĉĊċČčĎďĐđĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħĨĩĪīĬĭĮįİıĲĳĴĵĶķĸĹĺĻļĽľĿŀŁłŃńŅņŇňŉŊŋŌōŎŏŐőŒœŔŕŖŗŘřŚśŜŝŞşŠšŢţŤťŦŧŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽž"""
betabet = "bcdfghjkmprstvxz"
kibet="abcdefghijkmnpqrstvwxz0123456789"


class BitWriter:
    def __init__(self):
        self.bit_buffer = 0
        self.bit_count = 0
        self.output_bytes = bytearray()

    def write_bits(self, value: int, bits: int):
        """Přidá do bitového bufferu 'bits' bitů reprezentovaných hodnotou 'value'."""
        self.bit_buffer = (self.bit_buffer << bits) | value
        self.bit_count += bits
        while self.bit_count >= 8:
            shift = self.bit_count - 8
            byte = (self.bit_buffer >> shift) & 0xFF
            self.output_bytes.append(byte)
            self.bit_count -= 8
            self.bit_buffer &= (1 << self.bit_count) - 1

    def flush(self):
        """Doplňuje zbývající bity jedničkami a dokončí zápis do bytového pole."""
        if self.bit_count > 0:
            # Vypočítáme počet chybějících bitů do bajtu a vytvoříme masku s odpovídajícím počtem jedniček.
            pad = (1 << (8 - self.bit_count)) - 1
            self.output_bytes.append((self.bit_buffer << (8 - self.bit_count)) | pad)
            self.bit_buffer = 0
            self.bit_count = 0

    def get_bytes(self) -> bytes:
        """Vrací bajtový řetězec obsahující zapsané bity."""
        return bytes(self.output_bytes)

    def to_reader(self) -> 'BitReader':
        return BitReader(self.get_bytes())


class BitReader:
    def __init__(self, data: bytes):
        self.data = data
        self.index = 0
        self.bit_buffer = 0
        self.bit_count = 0

    def bits_remaining(self) -> int:
        """Vrací počet zbývajících bitů, které lze přečíst."""
        return (len(self.data) - self.index) * 8 + self.bit_count

    def read_bits(self, n: int) -> int:
        """Přečte n bitů ze streamu a vrátí jejich hodnotu."""
        while self.bit_count < n:
            if self.index >= len(self.data):
                raise ValueError("Nedostatek dat při dekompresi.")
            self.bit_buffer = (self.bit_buffer << 8) | self.data[self.index]
            self.bit_count += 8
            self.index += 1
        shift = self.bit_count - n
        value = (self.bit_buffer >> shift) & ((1 << n) - 1)
        self.bit_count -= n
        self.bit_buffer &= (1 << self.bit_count) - 1
        return value

    def iter_by(self, n: int):
        while self.bits_remaining() >= n:
            yield self.read_bits(n)


def compress(text: str) -> BitWriter:
    """
    Komprimuje řetězec dle specifikovaného schématu:
      - Pokud je znak obsažen v abecedě (62 znaků), zapíše se jako 6-bitový kód.
      - Pokud je znak obsažen v druhé abecedě (255 znaků),
        zapíše se speciální značka 62 (6 bitů) a poté 8bitový kód.
      - Jinak (pro jiné znaky z BMP) se zapíše značka 63 (6 bitů) a následně 16bitový kód.

    Výsledný bajtový řetězec již neobsahuje hlavičku s délkou, protože dekomprese
    bude fungovat na základě paddingu doplněného jedničkami.
    """
    writer = BitWriter()

    for c in text:
        if c in alphabet:
            # 6-bitový kód – index znaku v abecedě (0 až 61)
            code = alphabet.index(c)
            writer.write_bits(code, 6)
        elif c in second_alphabet:
            code = second_alphabet.index(c)
            writer.write_bits(62, 6)
            writer.write_bits(code, 8)
        else:
            # Předpokládáme, že znak patří do BMP (<= 0xFFFF)
            codepoint = ord(c)
            if codepoint > 0xFFFF:
                raise ValueError(f"Znak {c} (U+{codepoint:04X}) není v BMP a není podporován.")
            # Speciální značka 63 + 16bitový kód
            writer.write_bits(63, 6)
            writer.write_bits(codepoint, 16)

    writer.flush()
    return writer

def bcode(text: str) -> str:
    reader = compress(text).to_reader()
    return "".join(betabet[b] for b in reader.iter_by(4))

def bdecode(text:str) -> str:
    writer = BitWriter()
    for c in text:
        writer.write_bits(betabet.index(c), 4)
    writer.flush()
    return decompress(writer.to_reader())

def decompress(reader: BitReader) -> str:
    """
    Dekomprimuje data vytvořená funkcí compress bez hlavičky.
    De-komprese probíhá tak, že se čtou 6-bitové kódy,
    a pokud není dostatek bitů pro kompletní další kód (podle pravidel),
    předpokládá se, že se jedná o padding a dekomprese se ukončí.
    """
    result = []

    while reader.bits_remaining() >= 6:
        # Pokud zbývajících bitů nestačí pro další platný kód, ukončíme dekompresi.
        code = reader.read_bits(6)
        if code < 62:
            result.append(alphabet[code])
        elif code == 62:
            if reader.bits_remaining() < 8:
                break
            extra = reader.read_bits(8)
            result.append(second_alphabet[extra])
        elif code == 63:
            if reader.bits_remaining() < 16:
                break
            extra = reader.read_bits(16)
            result.append(chr(extra))

    return "".join(result)

if __name__ == "__main__":
    name = "mytestfile_002.txt"
    ename = bcode(name)
    print(ename)
    rname = bdecode(ename)
    print(rname)
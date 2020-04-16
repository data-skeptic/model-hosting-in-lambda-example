def untokenize(r):
    sentence = r['Answer_predictions']
    result = ' '.join(sentence).replace(' , ',', ').replace(' .','.').replace(' !','!').replace(" ' ", "'")
    result = result.replace(' ?','?').replace(' : ',': ').replace(' \'', '\'')
    result = result[0].upper() + result[1:]
    result = result.replace("<PAD>","")
    result = result.strip()
    return result
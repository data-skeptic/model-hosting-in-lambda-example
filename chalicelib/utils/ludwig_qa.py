def untokenize(r, pred_key = 'Answer_predictions'):
    sentence = r[pred_key]
    result = ' '.join(sentence).replace(' , ',', ').replace(' .','.').replace(' !','!').replace(" ' ", "'")
    result = result.replace(' ?','?').replace(' : ',': ').replace(' \'', '\'')
    result = result[0].upper() + result[1:]
    result = result.replace("<PAD>","")
    result = result.strip()
    return result
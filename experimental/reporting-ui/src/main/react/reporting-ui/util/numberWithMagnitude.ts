// In powers of 10
const MAGNITUDES = {
  3: 'K',
  6: 'M',
  9: 'B',
  12: 'T',
}

export const numberWithMagnitude = (num: number, decimals: number, trailingZeros: boolean = false) => {
  let power = 0;
  let newNumber = num;
  while (newNumber >= 1000) {
      newNumber = newNumber / 1000;
      power += 3;
  }

  if (power === 0) {
      return num.toString();
  } else {
      const value = (newNumber.toFixed(decimals));
      
      return trailingZeros ?
          value.toString() + MAGNITUDES[power]
          : value.toString().replace(/\.0+$/, '') + MAGNITUDES[power]
  }
}
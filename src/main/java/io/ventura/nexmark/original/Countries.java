package io.ventura.nexmark.original;


import java.nio.charset.Charset;

public class Countries {
    public static final String COUNTRIES[] = {
            "United States", "Afghanistan", "Albania", "Algeria", "American Samoa",
            "Andorra", "Angola", "Anguilla", "Antarctica", "Antigua", "Argentina",
            "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahamas",
            "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize",
            "Benin", "Bermuda", "Bhutan", "Bolivia", "Botswana", "Brazil",
            "British Indian Ocean Territory", "British Virgin Islands",
            "Brunei Darussalam", "Bulgaria", "Burkina Faso", "Burundi",
            "Cacos Islands", "Cambodia", "Cameroon", "Canada", "Cape Verde",
            "Cayman Islands", "Central African Republic", "Chad", "Chile", "China",
            "Christmas Island", "Colombia", "Comoros", "Congo", "Cook Islands",
            "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark",
            "Djibouti", "Dominica", "Dominican Republic", "East Timor", "Ecuador",
            "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia",
            "Ethiopia", "Falkland Islands", "Faroe Islands", "Fiji", "Finland",
            "France", "French Guiana", "French Polynesia",
            "French Southern Territory", "Futuna Islands", "Gabon", "Gambia",
            "Georgia", "Germany", "Ghana", "Gibraltar", "Greece", "Greenland",
            "Grenada", "Guadeloupe", "Guam", "Guatemala", "Guinea", "Guyana", "Haiti",
            "Heard and Mcdonald Island", "Honduras", "Hong Kong", "Hungary",
            "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel",
            "Italy", "Ivory Coast", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya",
            "Kiribati", "Korea, Democratic People's Rep", "Korea, Republic Of",
            "Kuwait", "Kyrgyzstan", "Lao People's Democratic Republ", "Latvia",
            "Lebanon", "Lesotho", "Liberia", "Libyan Arab Jamahiriya", "Lithuania",
            "Luxembourg", "Macau", "Macedonia", "Madagascar", "Malawi", "Malaysia",
            "Maldives", "Mali", "Malta", "Marshall Islands", "Martinique",
            "Mauritania", "Mauritius", "Mayotte", "Mexico", "Micronesia",
            "Moldova, Republic Of", "Monaco", "Mongolia", "Montserrat", "Morocco",
            "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands",
            "Netherlands Antilles", "New Caledonia", "New Zealand", "Nicaragua",
            "Niger", "Nigeria", "Niue", "Norfolk Island", "Northern Mariana Islands",
            "Norway", "Oman", "Pakistan", "Palau", "Panama", "Papua New Guinea",
            "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Puerto Rico",
            "Qatar", "Reunion", "Romania", "Russian Federation", "Rwanda",
            "Saint Kitts", "Samoa", "San Marino", "Sao Tome", "Saudi Arabia",
            "Senegal", "Seychelles", "Sierra Leone", "Singapore", "Slovakia",
            "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Georgia",
            "Spain", "Sri Lanka", "St. Helena", "St. Lucia", "St. Pierre",
            "St. Vincent and Grenadines", "Sudan", "Suriname",
            "Svalbard and Jan Mayen Island", "Swaziland", "Sweden", "Switzerland",
            "Syrian Arab Republic", "Taiwan", "Tajikistan", "Tanzania", "Thailand",
            "Togo", "Tokelau", "Tonga", "Trinidad", "Tunisia", "Turkey", "Turkmenistan",
            "Turks Islands", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates",
            "United Kingdom", "Uruguay", "Us Minor Islands", "Us Virgin Islands",
            "Uzbekistan", "Vanuatu", "Vatican City State", "Venezuela", "Viet Nam",
            "Western Sahara", "Yemen", "Zaire", "Zambia", "Zimbabwe"
    };

    public static final int NUM_COUNTRIES = COUNTRIES.length;

	private static final Charset US_ASCII = Charset.forName("US-ASCII");
     public static final byte[][] COUNTRIES_32 = new byte[NUM_COUNTRIES][];
    static {
    	for (int i = 0; i < NUM_COUNTRIES; i++) {
    		COUNTRIES_32[i] = COUNTRIES[i].substring(0, Math.min(32, COUNTRIES[i].length())).getBytes(US_ASCII);
		}
	}
}

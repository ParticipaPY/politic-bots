from src.bot_detector.bot_detector import BotDetector

if __name__ == "__main__":
    myconf = '../config.json'
    # sample of users
    users_sample = ['Jo_s_e_', '2586c735ce7a431', 'kXXR9JzzPBrmSPj', '180386_sm',
                    'federicotorale2', 'VyfQXRgEXdFmF1X', 'AM_1080', 'CESARSANCHEZ553',
                    'Paraguaynosune', 'Solmelga', 'SemideiOmar', 'Mercede80963021', 'MaritoAbdo',
                    'SantiPenap', 'CESARSANCHEZ553', 'Paraguaynosune']
    bot_detector = BotDetector(myconf)
    bot_detector.compute_bot_probability(users_sample)
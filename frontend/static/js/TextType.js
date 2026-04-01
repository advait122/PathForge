class TextType {
    constructor(target, options = {}) {
        this.target = target;
        this.options = {
            text: [],
            texts: [],
            typingSpeed: 50,
            pauseDuration: 2000,
            deletingSpeed: 30,
            showCursor: true,
            hideCursorWhileTyping: false,
            cursorCharacter: "|",
            cursorBlinkDuration: 0.5,
            variableSpeedEnabled: false,
            variableSpeedMin: 60,
            variableSpeedMax: 120,
            loop: true,
            ...options,
        };

        const preferredTexts = Array.isArray(this.options.texts) && this.options.texts.length > 0
            ? this.options.texts
            : this.options.text;
        this.textArray = (Array.isArray(preferredTexts) ? preferredTexts : [preferredTexts])
            .map((item) => String(item ?? ""));
        if (this.textArray.length === 0) {
            this.textArray = [""];
        }

        this.currentTextIndex = 0;
        this.currentCharIndex = 0;
        this.displayedText = "";
        this.isDeleting = false;
        this.timeoutId = null;

        this.contentSpan = document.createElement("span");
        this.contentSpan.className = "text-type__content";

        this.cursorSpan = document.createElement("span");
        this.cursorSpan.className = "text-type__cursor";
        this.cursorSpan.textContent = this.options.cursorCharacter;
        this.cursorSpan.style.setProperty(
            "--cursor-blink-duration",
            `${this.options.cursorBlinkDuration}s`,
        );

        this.target.textContent = "";
        this.target.appendChild(this.contentSpan);
        if (this.options.showCursor) {
            this.target.appendChild(this.cursorSpan);
        }

        this.tick = this.tick.bind(this);
        this.tick();
    }

    getRandomTypingSpeed() {
        if (!this.options.variableSpeedEnabled) {
            return this.options.typingSpeed;
        }
        const min = Number(this.options.variableSpeedMin) || this.options.typingSpeed;
        const max = Number(this.options.variableSpeedMax) || this.options.typingSpeed;
        if (max <= min) {
            return min;
        }
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    render() {
        this.contentSpan.textContent = this.displayedText;
        if (this.options.showCursor && this.cursorSpan) {
            const currentFullText = this.textArray[this.currentTextIndex] ?? "";
            const shouldHideCursor = this.options.hideCursorWhileTyping
                && (this.currentCharIndex < currentFullText.length || this.isDeleting);
            this.cursorSpan.classList.toggle("text-type__cursor--hidden", shouldHideCursor);
        }
    }

    schedule(nextDelay) {
        clearTimeout(this.timeoutId);
        this.timeoutId = setTimeout(this.tick, Math.max(0, Number(nextDelay) || 0));
    }

    tick() {
        const currentFullText = this.textArray[this.currentTextIndex] ?? "";

        if (this.isDeleting) {
            if (this.currentCharIndex > 0) {
                this.currentCharIndex -= 1;
                this.displayedText = currentFullText.slice(0, this.currentCharIndex);
                this.render();
                this.schedule(this.options.deletingSpeed);
                return;
            }

            this.isDeleting = false;
            this.currentTextIndex += 1;
            if (this.currentTextIndex >= this.textArray.length) {
                if (this.options.loop) {
                    this.currentTextIndex = 0;
                } else {
                    this.currentTextIndex = this.textArray.length - 1;
                    return;
                }
            }
            this.schedule(this.options.typingSpeed);
            return;
        }

        if (this.currentCharIndex < currentFullText.length) {
            this.currentCharIndex += 1;
            this.displayedText = currentFullText.slice(0, this.currentCharIndex);
            this.render();
            this.schedule(this.getRandomTypingSpeed());
            return;
        }

        this.isDeleting = true;
        this.schedule(this.options.pauseDuration);
    }

    destroy() {
        clearTimeout(this.timeoutId);
        this.timeoutId = null;
    }
}

export default TextType;

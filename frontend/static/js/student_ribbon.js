(function () {
    const toolbar = document.querySelector("[data-student-toolbar]");
    if (!toolbar) {
        return;
    }

    const menus = Array.from(toolbar.querySelectorAll("[data-student-menu]"));
    const notificationCards = Array.from(toolbar.querySelectorAll("[data-notification-card]"));
    let openMenu = null;

    function syncNotificationCard(card, options) {
        const settings = options || {};
        const toggle = card.querySelector("[data-notification-toggle]");
        const body = card.querySelector(".student-notification-body");
        const isOpen = card.classList.contains("is-open");

        if (toggle) {
            toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
        }

        if (!body) {
            return;
        }

        const nextHeight = isOpen ? body.scrollHeight + "px" : "0px";

        if (settings.immediate) {
            const previousTransition = body.style.transition;
            body.style.transition = "none";
            body.style.maxHeight = nextHeight;
            void body.offsetHeight;
            body.style.transition = previousTransition;
            return;
        }

        body.style.maxHeight = nextHeight;
    }

    function openNotificationCard(card, options) {
        const settings = options || {};
        card.classList.add("is-open");
        syncNotificationCard(card, settings);

        if (settings.skipScroll) {
            return;
        }

        window.requestAnimationFrame(function () {
            card.scrollIntoView({
                block: "nearest",
                behavior: settings.immediate ? "auto" : "smooth",
            });
        });
    }

    function closeNotificationCard(card, options) {
        card.classList.remove("is-open");
        syncNotificationCard(card, options);
    }

    function closeMenu(menu) {
        if (!menu) {
            return;
        }
        const trigger = menu.querySelector("[data-student-menu-trigger]");
        const panel = menu.querySelector("[data-student-menu-panel]");
        menu.classList.remove("is-open");
        if (trigger) {
            trigger.setAttribute("aria-expanded", "false");
        }
        if (panel) {
            panel.hidden = true;
        }
        if (openMenu === menu) {
            openMenu = null;
        }
    }

    function closeAllMenus(exceptMenu) {
        menus.forEach(function (menu) {
            if (menu !== exceptMenu) {
                closeMenu(menu);
            }
        });
    }

    function openMenuPanel(menu) {
        const trigger = menu.querySelector("[data-student-menu-trigger]");
        const panel = menu.querySelector("[data-student-menu-panel]");
        closeAllMenus(menu);
        menu.classList.add("is-open");
        if (trigger) {
            trigger.setAttribute("aria-expanded", "true");
        }
        if (panel) {
            panel.hidden = false;
        }
        openMenu = menu;

        notificationCards.forEach(function (card) {
            if (card.classList.contains("is-open")) {
                syncNotificationCard(card);
            }
        });
    }

    menus.forEach(function (menu) {
        const trigger = menu.querySelector("[data-student-menu-trigger]");
        if (!trigger) {
            return;
        }

        trigger.addEventListener("click", function (event) {
            event.preventDefault();
            const isOpen = menu.classList.contains("is-open");
            if (isOpen) {
                closeMenu(menu);
                return;
            }
            openMenuPanel(menu);
        });
    });

    notificationCards.forEach(function (card) {
        const toggle = card.querySelector("[data-notification-toggle]");
        if (!toggle) {
            return;
        }

        syncNotificationCard(card, { immediate: true });

        toggle.addEventListener("click", function () {
            const shouldOpen = !card.classList.contains("is-open");
            notificationCards.forEach(function (otherCard) {
                if (otherCard !== card) {
                    closeNotificationCard(otherCard);
                }
            });

            if (shouldOpen) {
                openNotificationCard(card);
                return;
            }

            closeNotificationCard(card);
        });
    });

    window.addEventListener("resize", function () {
        notificationCards.forEach(function (card) {
            if (card.classList.contains("is-open")) {
                syncNotificationCard(card, { immediate: true });
            }
        });
    });

    document.addEventListener("click", function (event) {
        if (!openMenu) {
            return;
        }
        if (openMenu.contains(event.target)) {
            return;
        }
        closeMenu(openMenu);
    });

    document.addEventListener("focusin", function (event) {
        if (!openMenu) {
            return;
        }
        if (openMenu.contains(event.target)) {
            return;
        }
        closeMenu(openMenu);
    });

    document.addEventListener("keydown", function (event) {
        if (event.key !== "Escape" || !openMenu) {
            return;
        }
        const trigger = openMenu.querySelector("[data-student-menu-trigger]");
        closeMenu(openMenu);
        if (trigger) {
            trigger.focus();
        }
    });
})();

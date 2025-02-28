function openLoginPopup() {
    document.getElementById('login-popup').style.display = 'block';
}

function closeLoginPopup() {
    document.getElementById('login-popup').style.display = 'none';
}

function requestOTP() {
    const phone = document.getElementById('phone').value;
    if (!phone) {
        alert('Please enter your phone number first');
        return;
    }

    jQuery.ajax({
        url: ajax_object.ajax_url,
        type: 'POST',
        data: {
            action: 'request_otp',
            phone: phone
        },
        success: function (response) {
            if (response.success) {
                alert('OTP has been sent to your phone');
            } else {
                alert('Failed to send OTP. Please try again.');
            }
        },
        error: function () {
            alert('Failed to send OTP. Please try again.');
        }
    });
}

function handleSubmit(event) {
    event.preventDefault();

    const phone = document.getElementById('phone').value;
    const otp = document.getElementById('otp').value;

    jQuery.ajax({
        url: ajax_object.ajax_url,
        type: 'POST',
        data: {
            action: 'verify_login',
            phone: phone,
            otp: otp
        },
        success: function (response) {
            if (response.success) {

                createCookie("wapcar_token", response.data.token, "10");

                function createCookie(name, value, days) {
                    let expires;

                    if (days) {
                        let date = new Date();
                        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
                        expires = "; expires=" + date.toGMTString();
                    }
                    else {
                        expires = "";
                    }
                    document.cookie = escape(name) + "=" +
                        escape(value) + expires + "; path=/";
                }

                window.location.reload();
            } else {
                alert('Invalid OTP. Please try again.');
            }
        },
        error: function () {
            alert('Login failed. Please try again.');
        }
    });
}

function handleGoogleSignIn(response) {
    jQuery.ajax({
        url: ajax_object.ajax_url,
        type: 'POST',
        data: {
            action: 'handle_google_signin',
            credential: response.credential,
            nonce: ajax_object.google_signin_nonce
        },
        success: function (response) {
            if (response.success) {
                createCookie("wapcar_token", response.data.token, "10");

                function createCookie(name, value, days) {
                    let expires;

                    if (days) {
                        let date = new Date();
                        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
                        expires = "; expires=" + date.toGMTString();
                    }
                    else {
                        expires = "";
                    }
                    document.cookie = escape(name) + "=" +
                        escape(value) + expires + "; path=/";
                }
                window.location.reload();
            } else {
                alert('Google login failed: ' + (response.data || 'Unknown error'));
            }
        },
        error: function (xhr, status, error) {
            console.error('Login error:', error);
            alert('Login failed. Please try again.');
        }
    });
}

function initializeGoogleSignIn() {
    google.accounts.id.initialize({
        client_id: ajax_object.google_client_id,
        callback: handleGoogleSignIn,
        auto_select: false,
        cancel_on_tap_outside: true
    });

    google.accounts.id.renderButton(
        document.getElementById('google-signin-button'),
        {
            theme: 'outline',
            size: 'large',
            width: '100%',
            text: 'continue_with'
        }
    );
}

// Initialize Google Sign-In after the page loads
window.onload = function () {
    initializeGoogleSignIn();
};


// Close popup when clicking outside
window.onclick = function (event) {
    const popup = document.getElementById('login-popup');
    if (event.target === popup) {
        closeLoginPopup();
    }
}
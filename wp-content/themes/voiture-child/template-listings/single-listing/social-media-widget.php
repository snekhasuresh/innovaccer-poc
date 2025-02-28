<?php
function custom_share_buttons()
{
    ob_start();
?>
    <style>
        .share-container {
            transform: translateY(-50%);
            width: fit-content;
            margin-left: 20px;
            height: 0;
        }

        .share-buttons {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 10px;
        }

        /* Add line separator */
        .separator {
            width: 20px;
            height: 1px;
            background-color: #e0e0e0;
            margin: 10px 0;
        }

        .share-button {
            width: 40px;
            height: 40px;
            border-radius: 50%;
            background: white;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            transition: all 0.3s ease;
            color: #666;
            text-decoration: none;
            position: relative;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1) !important;
        }

        .share-button:hover {
            transform: scale(1.1);
        }

        /* Icon styling */
        .icon-container {
            width: 24px;
            height: 24px;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        /* Number indicator styling */
        .number-indicator {
            position: absolute;
            top: -8px;
            right: -8px;
            background: #666;
            color: white;
            border-radius: 12px;
            padding: 2px 6px;
            font-size: 11px;
            font-family: Arial, sans-serif;
            display: flex;
            align-items: center;
            justify-content: center;
            min-width: 20px;
            height: 16px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        /* Specific color styles for each platform */
        .share-button.whatsapp:hover .icon-container svg {
            color: #25D366;
            /* WhatsApp green */
        }

        .share-button.twitter:hover .icon-container svg {
            color: #1DA1F2;
            /* Twitter blue */
        }

        .share-button.pinterest:hover .icon-container svg {
            color: #E60023;
            /* Pinterest red */
        }

        .share-button.facebook:hover .icon-container svg {
            color: #1877F2;
            /* Facebook blue */
        }
    </style>

    <div class="share-container">
        <div class="share-buttons">
            <div class="like-button">
                <a href="#" class="share-button">
                    <div class="icon-container">
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M6.633 10.25c.806 0 1.533-.446 2.031-1.08a9.041 9.041 0 0 1 2.861-2.4c.723-.384 1.35-.956 1.653-1.715a4.498 4.498 0 0 0 .322-1.672V2.75a.75.75 0 0 1 .75-.75 2.25 2.25 0 0 1 2.25 2.25c0 1.152-.26 2.243-.723 3.218-.266.558.107 1.282.725 1.282m0 0h3.126c1.026 0 1.945.694 2.054 1.715.045.422.068.85.068 1.285a11.95 11.95 0 0 1-2.649 7.521c-.388.482-.987.729-1.605.729H13.48c-.483 0-.964-.078-1.423-.23l-3.114-1.04a4.501 4.501 0 0 0-1.423-.23H5.904m10.598-9.75H14.25M5.904 18.5c.083.205.173.405.27.602.197.4-.078.898-.523.898h-.908c-.889 0-1.713-.518-1.972-1.368a12 12 0 0 1-.521-3.507c0-1.553.295-3.036.831-4.398C3.387 9.953 4.167 9.5 5 9.5h1.053c.472 0 .745.556.5.96a8.958 8.958 0 0 0-1.302 4.665c0 1.194.232 2.333.654 3.375Z" />
                        </svg>
                    </div>
                    <span class="number-indicator">432</span>
                </a>
            </div>
            <a href="#" class="share-button">
                <div class="icon-container">
                    <!-- Bookmark icon -->
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M6.75 3A1.75 1.75 0 0 0 5 4.75v15.447a.75.75 0 0 0 1.175.631l5.328-3.84a.25.25 0 0 1 .294 0l5.328 3.84a.75.75 0 0 0 1.175-.63V4.75A1.75 1.75 0 0 0 17.25 3H6.75z" />
                    </svg>
                </div>
            </a>
            <div class="separator"></div>
            <a href="#" class="share-button facebook">
                <div class="icon-container">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M22 2.75C22 1.78 21.22 1 20.25 1h-16.5C2.78 1 2 1.78 2 2.75v16.5c0 .97.78 1.75 1.75 1.75h8.586V14.5h-2.5v-3h2.5V9.271c0-2.466 1.507-3.807 3.707-3.807 1.054 0 1.96.078 2.223.113v2.577h-1.526c-1.195 0-1.428.57-1.428 1.402V11.5h2.858l-.37 3H15.5v6.5h4.75c.97 0 1.75-.78 1.75-1.75v-16.5z" />
                    </svg>
                </div>
            </a>
            <a href="#" class="share-button whatsapp">
                <div class="icon-container">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M12.034 2C6.525 2 2 6.494 2 11.999c0 1.808.474 3.58 1.38 5.175L2 22l5.931-1.376a10.3 10.3 0 0 0 4.105.873c5.509 0 10.034-4.494 10.034-9.999C22 6.495 17.543 2 12.034 2zm5.827 14.537c-.262.744-1.015 1.335-1.932 1.49-.52.092-1.148.166-3.32-.691-2.712-1.08-4.448-3.731-4.586-3.902-.13-.17-1.092-1.482-1.092-2.827 0-1.344.69-2.006.936-2.282.246-.276.537-.34.716-.34.18 0 .358.005.514.01.167.006.39-.064.61.468.262.611.89 2.102.965 2.253.075.15.125.326.024.496-.097.173-.15.278-.293.43-.146.148-.304.331-.435.477-.147.16-.3.334-.257.53.043.195.416 1.047 1.112 1.698.762.719 1.407.936 1.617 1.04.21.105.33.09.453-.053.13-.15.568-.665.719-.894.15-.23.297-.193.5-.12.204.075 1.287.6 1.51.708.223.11.373.167.429.26.05.085.05.499-.211 1.242z" />
                    </svg>
                </div>
            </a>
            <a href="#" class="share-button twitter">
                <div class="icon-container">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M22 5.768a7.966 7.966 0 0 1-2.357.653 4.13 4.13 0 0 0 1.765-2.294 7.977 7.977 0 0 1-2.593 1.018 3.969 3.969 0 0 0-6.767 3.617 11.368 11.368 0 0 1-8.3-4.163 3.934 3.934 0 0 0-.543 2.001c0 1.38.695 2.598 1.76 3.308a3.948 3.948 0 0 1-1.8-.497v.05c0 1.928 1.362 3.545 3.165 3.91a4.051 4.051 0 0 1-1.791.07c.504 1.594 1.97 2.755 3.704 2.785A8.046 8.046 0 0 1 2 18.05a11.337 11.337 0 0 0 6.29 1.869c7.547 0 11.675-6.381 11.675-11.92 0-.18-.004-.36-.013-.54A8.434 8.434 0 0 0 22 5.769z" />
                    </svg>
                </div>
            </a>
            <a href="#" class="share-button pinterest">
                <div class="icon-container">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M12 0C5.373 0 0 5.373 0 12c0 4.874 3.554 8.937 8.218 9.884-.113-.839-.213-2.128.045-3.044.23-.875 1.48-5.56 1.48-5.56s-.378-.756-.378-1.874c0-1.753 1.018-3.06 2.285-3.06 1.077 0 1.598.81 1.598 1.78 0 1.084-.692 2.704-1.048 4.21-.298 1.258.635 2.282 1.88 2.282 2.258 0 3.998-2.38 3.998-5.804 0-3.033-2.18-5.157-5.292-5.157-3.605 0-5.725 2.703-5.725 5.494 0 1.084.42 2.25.945 2.88.104.122.12.228.09.352-.097.384-.308 1.218-.35 1.387-.055.227-.18.277-.42.167-1.565-.69-2.544-2.845-2.544-4.575 0-3.734 2.712-7.168 7.833-7.168 4.104 0 7.294 2.93 7.294 6.837 0 4.073-2.558 7.352-6.108 7.352-1.19 0-2.307-.618-2.688-1.308l-.733 2.794c-.264 1.012-.982 2.274-1.467 3.046A12.004 12.004 0 0 0 12 24c6.627 0 12-5.373 12-12S18.627 0 12 0z" />
                    </svg>
                </div>
            </a>
            <a href="#" class="share-button link">
                <div class="icon-container">
                    <!-- Link/Attachment icon -->
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M10.828 9.172a4 4 0 0 1 5.657 0l1.415 1.414a4 4 0 0 1 0 5.657l-4.95 4.95a4 4 0 1 1-5.657-5.657L8.414 14M8.707 6.343a4 4 0 0 1 5.657 0l1.415 1.414a4 4 0 0 1 0 5.657l-4.95 4.95a4 4 0 1 1-5.657-5.657L8.414 8" />
                    </svg>
                </div>
            </a>
        </div>
    </div>

<?php
    return ob_get_clean();
}
add_shortcode('share_buttons', 'custom_share_buttons');

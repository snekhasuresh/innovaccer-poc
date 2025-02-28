<?php
function nearest_dealers_shortcode()
{
    ob_start();
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;

    // get listing post by post name
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));
    // Example dealer data
    $dealers = [
        [
            'name' => 'Peringgit Sri Motor Sdn. Bhd. (Taman Equin...)',
            'address' => 'No. 18 and 20, Jalan Equine 1H, Taman Equine',
            'certified' => true,
        ],
        [
            'name' => 'Sag Ultimate Sdn Bhd (3S Centre)',
            'address' => 'Lot 35710 Batu 22 Jalan Beranang, 43500 Semenyih',
            'certified' => true,
        ],
        [
            'name' => 'Accord Auto Sdn Bhd',
            'address' => 'Lot 42 and 43, Jalan 11 Kawasan Kilang, Off Jalan Pandan Indah Kampung Baru Ampang',
            'certified' => true,
        ],
        [
            'name' => 'Angkasa Motor Sdn Bhd (3S Centre)',
            'address' => '558, Batu 3 1/2, Jalan Ipoh, 51200 Kuala Lumpur',
            'certified' => true,
        ],
    ];

?>

    <title>Honda Dealers</title>
    <style>
        .container-dealer {
            margin: 0 auto;
            background: white;
            border-radius: 8px;
        }

        .page-title {
            position: relative;
            line-height: 32px;
            margin-bottom: 15px;
            margin-top: 10px;
        }

        .tabs-dealer {
            display: flex;
            gap: 16px;
            border-bottom: 1px solid #e0e0e0;
            margin-bottom: 24px;
            position: relative;
        }

        .tab-dealer {
            padding: 10px 0px;
            border: none;
            background: none;
            cursor: pointer;
            color: #8c8c8c;
            font-size: 14px;
            font-weight: 700;
            position: relative;
            font-family: 'roboto';
        }

        .tab-dealer.active {
            color: #262626;
            font-weight: 700;
            font-size: 14px;
        }

        .tab-dealer.active::after {
            content: '';
            position: absolute;
            bottom: -1px;
            left: 0;
            width: 100%;
            height: 3px;
            background: #00bcd4;
        }

        .dealers-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, 485px);
            gap: 30px;
            justify-content: start;
        }

        .dealer-card {
            width: 100%;
            height: 163px;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 16px;
            position: relative;
            display: flex;
            flex-direction: column;
        }

        .dealer-header {
            display: flex;
            gap: 8px;
            margin-bottom: 8px;
            padding-right: 32px;
            align-items: center;
        }

        .certified-badge {
            font-family: "Roboto";
            font-weight: Bold;
            font-size: 10px;
            background: #00bcd4;
            color: white;
            padding: 2px 3px;
            white-space: nowrap;
            height: 15px;
            border-radius: 3px;
            display: flex;
            align-items: center;
        }

        .dealer-name {
            width: 100%;
            font-family: "Roboto";
            font-weight: bold;
            font-size: 16px;
            color: #262626;
            line-height: 22px;
            vertical-align: bottom;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .chevron-right {
            position: absolute;
            top: 16px;
            right: 16px;
            color: #999;
            width: 20px;
            height: 20px;
            cursor: pointer;
        }

        .dealer-address {
            color: #666;
            margin-bottom: auto;
            line-height: 1.5;
            font-size: 14px;
            display: flex;
            align-items: baseline;
        }

        .dealer-actions {
            display: flex;
            gap: 16px;
            margin-top: 16px;
        }

        .dealer-btn {
            flex: 1;
            height: 40px;
            border: 1px solid #e0e0e0;
            border-radius: 5px;
            background: white;
            cursor: pointer;
            font-weight: 500;
            transition: background-color 0.2s;
            font-size: 14px;
        }

        .dealer-btn:hover {
            background: #f5f5f5;
        }

        .dealer-btn-offer {
            color: #00bcd4;
        }

        .dealer-btn-offer:hover {
            background: #e0f7fa;
        }

        .view-more {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
            margin: 24px auto 0;
            padding: 8px 16px;
            border: none;
            background: none;
            color: #576B95;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }

        .view-more svg {
            width: 18px;
            height: 20px;
        }


        .address {
            width: 100%;
            font-family: "Roboto";
            font-size: 13px;
            color: #8c8c8c;
            line-height: 20px;
            vertical-align: middle;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
            padding-right: 34px;
            height: 40px;
        }

        .view-more svg {
            width: 18px;
            height: 20px;
        }

        .address-icon {
            width: 18px;
            margin-right: 12px;
        }

        @media (max-width: 880px) {
            .dealers-grid {
                grid-template-columns: 1fr;
            }

            .dealer-card {
                width: 100%;
                max-width: 424px;
                margin: 0 auto;
            }
        }
    </style>

    <div class="container-dealer">
        <h2 class="page-title wa-title-text"><?php esc_html_e('Nearest ' . $listing_post[0]->post_title . ' Dealers', 'voiture'); ?></h2>

        <div class="tabs-dealer">
            <button class="tab-dealer active">Showroom</button>
            <button class="tab-dealer">Service</button>
            <button class="tab-dealer">Body & Paint</button>
        </div>

        <div class="dealers-grid">
            <?php foreach ($dealers as $dealer) : ?>
                <div class="dealer-card">
                    <div class="dealer-header">
                        <?php if ($dealer['certified']) : ?>
                            <span class="certified-badge">Carlist</span>
                        <?php endif; ?>
                        <div class="dealer-name"><?php echo $dealer['name']; ?></div>
                    </div>
                    <svg class="chevron-right" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                    </svg>
                    <div class="dealer-address">
                        <div class="address-icon">
                            <img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyB3aWR0aD0iMjJweCIgaGVpZ2h0PSIyMnB4IiB2aWV3Qm94PSIwIDAgMjIgMjIiIHZlcnNpb249IjEuMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayI+CiAgICA8dGl0bGU+MUJBNDhBRUEtRjg2My00QjgyLUEzQzctNzVDRDAwOEJEQTZEPC90aXRsZT4KICAgIDxnIGlkPSLovabns7vovablnosiIHN0cm9rZT0ibm9uZSIgc3Ryb2tlLXdpZHRoPSIxIiBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPgogICAgICAgIDxnIGlkPSLnu4/plIDllYZf6K+m5oOF6aG1LeWIhuexuyIgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTE4LjAwMDAwMCwgLTM3NS4wMDAwMDApIiBmaWxsPSIjN0Y3RjdGIiBmaWxsLXJ1bGU9Im5vbnplcm8iPgogICAgICAgICAgICA8ZyBpZD0i57yW57uELTIiIHRyYW5zZm9ybT0idHJhbnNsYXRlKDE4LjAwMDAwMCwgMzY2LjAwMDAwMCkiPgogICAgICAgICAgICAgICAgPGcgaWQ9Iue8lue7hC00Ij4KICAgICAgICAgICAgICAgICAgICA8ZyBpZD0i5L2N572uIiB0cmFuc2Zvcm09InRyYW5zbGF0ZSgwLjAwMDAwMCwgOS4wMDAwMDApIj4KICAgICAgICAgICAgICAgICAgICAgICAgPHBhdGggZD0iTTIwLjAyNTc4NDEsMTUuMjQ1OTc3MyBDMTkuMjQ2NzYyMiwxNC42ODU2ODM0IDE4LjEzOTUwNDgsMTQuMjI2MzgyMyAxNi43MzQ5MzIyLDEzLjg4MTQxMTEgTDE2LjczNDkzMjIsMTMuODgxNDExMSBDMTYuNDAzOTA1NCwxMy43ODUzMDYxIDE2LjA1NzY0NjcsMTMuOTc1NzQ3NyAxNS45NjE1NDIsMTQuMzA2Nzc1MiBDMTUuODY1NDM3MSwxNC42Mzc4MDIgMTYuMDU1ODc4NiwxNC45ODQwNjA3IDE2LjM4NjkwNjEsMTUuMDgwMTY1NCBDMTYuNDAzNDQ0NywxNS4wODQ5NjcgMTYuNDIwMTc0OSwxNS4wODkwODE5IDE2LjQzNzA1MzksMTUuMDkyNDk5OCBDMTguNzQzMTkzMiwxNS42NTk4ODcgMjAuMTE5OTU5NywxNi41NTYzNTk0IDIwLjExOTk1OTcsMTcuNDkyODMzOSBDMjAuMTE5OTU5NywxOC4yNjk1ODcgMTkuMTMxMDA0NiwxOS4wMzkyNDY4IDE3LjQwNjcwNDgsMTkuNjA0NjQ5OSBDMTUuNjYzNDA2NywyMC4xNzYyOTIzIDEzLjI4MzIyNDQsMjAuNTA0MjQzIDEwLjg3NjkyMDgsMjAuNTA0MjQzIEM4LjQ2NTUxODY3LDIwLjUwNDI0MyA2LjA4MjQ5ODI0LDIwLjE3NjI5MjMgNC4zNDE3NjIxLDE5LjYwNDY0OTkgQzIuNjIwNTkzNiwxOS4wNDA5NTA2IDEuNjMzNjI2OTQsMTguMjcxMjkwOCAxLjYzMzYyNjk0LDE3LjQ5Mzk3MDQgQzEuNjMzNjI2OTQsMTYuNTcxOTY3MiAzLjAwMzAxNTUzLDE1LjY3ODMzMDkgNS4yOTYzOTM2LDE1LjEwMzI4MSBMNS4yOTYzOTM2MSwxNS4xMDMyODEgQzUuNjMwNzQ3MjYsMTUuMDE5NDU3MyA1LjgzMzg0NDEyLDE0LjY4MDQ1NzYgNS43NTAwMjA0MywxNC4zNDYxMDE5IEM1LjY2NjE5Njc1LDE0LjAxMTc0ODIgNS4zMjcxOTcwOSwxMy44MDg2NTE0IDQuOTkyODQxMzIsMTMuODkyNDc1MSBDNC45OTI4NDEzMiwxMy44OTI0NzUxIDQuOTkyODQxMywxMy44OTI0NzUxIDQuOTkyODQxMywxMy44OTI0NzUxIEMzLjU5NjIxODIsMTQuMjQyODM1OSAyLjQ5NjMzNjYzLDE0LjcwMzI3MTMgMS43MjEyNzg3NSwxNS4yNjEyOTY0IEMwLjYxNzE0NDE4NCwxNi4wNTcwNTY0IDAuMzg1MzY2MTA1LDE2LjkxNDk0NTggMC4zODUzNjYxMDUsMTcuNDkzOTc0NyBDMC4zODUzNjYxMDUsMTguNDY0NDkwMSAxLjAwNDEwMTY3LDE5LjgyNDIyNTYgMy45NTI1MzU3OCwyMC43OTE5MjIgQzUuODE0NzAxMzQsMjEuNDAyOTk3IDguMzM4NDQwNTMsMjEuNzUzNjQyNSAxMC44NzY5MjI5LDIxLjc1MzY0MjUgQzEzLjQxMDAzMDYsMjEuNzUzNjQyNSAxNS45MzE3NzI5LDIxLjQwMjk5NyAxNy43OTU2Mzc5LDIwLjc5MTkyMiBDMjAuNzQ4NjA3NiwxOS44MjM2NzUzIDIxLjM2ODE4MjMsMTguNDY1NjQzNyAyMS4zNjgxODIzLDE3LjQ5Mzk3NDcgQzIxLjM2ODE4MjMsMTYuOTA4MTQ3OCAyMS4xMzUyNjk4LDE2LjA0NDU4NDIgMjAuMDI1NzQzNiwxNS4yNDU5Nzk3IEwyMC4wMjU3ODQxLDE1LjI0NTk3NzMgWiIgaWQ9Iui3r+W+hCI+PC9wYXRoPgogICAgICAgICAgICAgICAgICAgICAgICA8cGF0aCBkPSJNMTAuMDg3NzEyNiwxOC41MTY2OTAxIEwxMC4xMDgxMzg2LDE4LjU0MTA4NzcgTDEwLjEwODEzODYsMTguNTQxMDg3NyBDMTAuMzAxMDE1NywxOC43NjI5NDI4IDEwLjU4MDEzOTEsMTguODkwOTI2IDEwLjg3NDExMDUsMTguODkyMzYzNSBMMTAuODc0MTEwNSwxOC44OTIzNjM1IEMxMS4xNzgzMDAxLDE4Ljg5NTc5NjMgMTEuNDY1ODY2NSwxOC43NTM4MDUgMTEuNjQ4MDI3NiwxOC41MTAxNjUzIEMxMi4wNjg0NjEsMTguMDAyNjM2NiAxMy41MjMyNDMyLDE2LjIxMzk0NjkgMTQuOTM2ODgyNiwxNC4xMDQxMjE0IEMxNi45ODE0NjIsMTEuMDUxNTgyMiAxOC4wMTg5Mjk0LDguNzA5NDEzNTUgMTguMDE4OTI5NCw3LjE0MzcwNDg3IEMxOC4wMTg5Mjk0LDMuMjA1NzQ2MzUgMTQuODA5NzgxMSwxLjgyNTcyMDU3ZS0wNSAxMC44NjQ3NDg0LDEuODI1NzIwNTdlLTA1IEM2LjkxOTcxNTY2LDEuODI1NzIwNTdlLTA1IDMuNzEwNTY3MzIsMy4yMDU3NDYzNSAzLjcxMDU2NzMyLDcuMTQzNzA0ODcgQzMuNzEwNTY3MzIsOC43MDk0MDkzMSA0Ljc0ODYwMTg4LDExLjA1MzI4MTcgNi43OTY4NjI5MSwxNC4xMDcyNDQzIEM4LjIxNTMzMDk5LDE2LjIyNzI4NTkgOS42NzQzNzQ2OCwxOC4wMTc5NjgzIDEwLjA4NzcxNDgsMTguNTE2Njg3MSBMMTAuMDg3NzEyNiwxOC41MTY2OTAxIFogTTEwLjg2NDc1MDQsMS4yNDgyNjI5NiBDMTQuMTIxNTQ4NiwxLjI0ODI2Mjk2IDE2Ljc3MDY4OTgsMy44OTI4NTc5OSAxNi43NzA2ODk4LDcuMTQzNzA3ODYgQzE2Ljc3MDY4OTgsOS42MjI4OTg4NSAxMy4xNDM2NzY0LDE0LjcxMDM3OTMgMTAuODY0NzUwNCwxNy40OTY4MTkgQzguNTg1ODQ1NzUsMTQuNzEwNjU1NCA0Ljk1ODgxMTEyLDkuNjIzNzQ4NiA0Ljk1ODgxMTEyLDcuMTQzNzA3ODYgQzQuOTU4ODExMTIsMy44OTI4NTc5OSA3LjYwODIyODQ4LDEuMjQ4MjYyOTYgMTAuODY0NzUwNCwxLjI0ODI2Mjk2IFoiIGlkPSLlvaLnirYiPjwvcGF0aD4KICAgICAgICAgICAgICAgICAgICAgICAgPHBhdGggZD0iTTE0LjQ3MDUxNTgsNi44NTQ2MDEzMyBDMTQuNDcwNTE1OCw0Ljg3ODY3NTM1IDEyLjg1ODU0NjcsMy4yNzA5ODg5NyAxMC44NzY5NDAxLDMuMjcwOTg4OTcgQzguODk1MzM5OTMsMy4yNzA5ODg5NyA3LjI4MzEwOTUxLDQuODc5NTMxNDggNy4yODMxMDk1MSw2Ljg1NDYwMTMzIEw3LjI4MzEwOTUxLDYuODU0NjE1OTkgQzcuMjg2MDkyMzcsOC44MzY1MDI5NiA4Ljg5NTA2NTkxLDEwLjQ0MDc1NjQgMTAuODc2OTYxNCwxMC40Mzc5MzQ3IEMxMi44NTg1NjE2LDEwLjQzNzkzNDcgMTQuNDcwNTE1OCw4LjgzMDUyMjg0IDE0LjQ3MDUxNTgsNi44NTQ1OTQ3NCBMMTQuNDcwNTE1OCw2Ljg1NDYwMTMzIFogTTkuMjE3OTA3NTMsOC41MDU0MTQ3IEw5LjIxNzkwNzQyLDguNTA1NDE0NTggQzguNzc1MjE2ODksOC4wNzA2Mzk0OSA4LjUyNzUzMzYsNy40NzUwNzY2NCA4LjUzMTMyNDIzLDYuODU0NjAxMjEgQzguNTMxMzI0MjMsNS41NjY5MTYxOCA5LjU4MzU4NzEzLDQuNTE5MjMwNTcgMTAuODc2OTU3LDQuNTE5MjMwNTcgQzEyLjE3MDMxNjIsNC41MTkyMzA1NyAxMy4yMjIyNDg1LDUuNTY2OTExOTMgMTMuMjIyMjQ4NSw2Ljg1NDYwMTIxIEMxMy4yMjIyNDg1LDguMTQyMjg2MjQgMTIuMTcwMzEyLDkuMTg5NzA2MTEgMTAuODc2OTU3LDkuMTg5NzA2MTEgTDEwLjg3Njk1NzEsOS4xODk3MDYxMSBDMTAuMjU2NDY2OCw5LjE5MjkxMjQ4IDkuNjYwNDgxMjIsOC45NDc3MzU0NiA5LjIyMTg4ODYyLDguNTA4ODA5MzQgTDkuMjE3OTA3NTMsOC41MDU0MTQ3IFoiIGlkPSLlvaLnirYiPjwvcGF0aD4KICAgICAgICAgICAgICAgICAgICA8L2c+CiAgICAgICAgICAgICAgICA8L2c+CiAgICAgICAgICAgIDwvZz4KICAgICAgICA8L2c+CiAgICA8L2c+Cjwvc3ZnPgo=" alt="" srcset="">
                        </div>
                        <div class="address">
                            <?php echo $dealer['address']; ?>
                        </div>
                    </div>

                    <div class="dealer-actions">
                        <button class="dealer-btn">
                            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" width="16" height="16" style="vertical-align: middle; margin-right: 8px;">
                                <path d="M22 16.92v3a2 2 0 01-2.18 2 19.77 19.77 0 01-8.63-3.16A19.54 19.54 0 014.24 9.81 19.77 19.77 0 011.08 1.18 2 2 0 013.06 0h3a2 2 0 012 1.72 13.35 13.35 0 00.7 2.89 2 2 0 01-.45 2.11l-1.27 1.27a16 16 0 007.12 7.12l1.27-1.27a2 2 0 012.11-.45 13.35 13.35 0 002.89.7A2 2 0 0122 16.92z"></path>
                            </svg>
                            Contact
                        </button>

                        <button class="dealer-btn dealer-btn-offer">Avail Offer</button>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>

        <button class="view-more">
            View More
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
            </svg>
        </button>
    </div>

    <script>
        const tabs = document.querySelectorAll('.tab-dealer');
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                tabs.forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
            });
        });

        const buttons = document.querySelectorAll('.dealer-btn');
        buttons.forEach(button => {
            button.addEventListener('click', () => {
                console.log('Button clicked:', button.textContent);
            });
        });

        const viewMore = document.querySelector('.view-more');
        viewMore.addEventListener('click', () => {
            console.log('View more clicked');
        });
    </script>

<?php
    return ob_get_clean();
}

add_shortcode('nearest_dealers', 'nearest_dealers_shortcode');

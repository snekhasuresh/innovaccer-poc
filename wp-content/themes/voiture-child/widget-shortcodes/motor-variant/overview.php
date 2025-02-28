<?php
function enqueue_motor_variant_overview_css()
{
    wp_enqueue_style('overview-gallery-style', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/gallery.css', array(), '1.0', 'all');
    wp_enqueue_style('overview-ownership-style', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/ownership-cost.css', array(), '1.0', 'all');
}

function motor_variant_overview_shortcode()
{
    enqueue_motor_variant_overview_css();

    $make = get_query_var('make');
    $model = get_query_var('model');

    $global_variant_post_data = get_motor_variant_from_query_vars();
	print_r($global_variant_post_data);
    $variant_post = $global_variant_post_data['variant_post'];
print_r($variant_post);
    $variant_post_title = $variant_post->post_title;
    $variant_post_meta = $global_variant_post_data['variant_post_meta'];
    $image_url = $global_variant_post_data['variant_image_url'];

    $retail_price = isset($variant_post_meta['price'][0]) ? 'THB ' . number_format($variant_post_meta['price'][0]) : 'ยังไม่คอนเฟิร์ม';
    $road_tax = isset($variant_post_meta['road_tax'][0]) ? 'THB ' . number_format($variant_post_meta['road_tax'][0]) : 'ยังไม่คอนเฟิร์ม';
    $insurance = isset($variant_post_meta['insurance'][0]) ? 'THB ' . number_format($variant_post_meta['insurance'][0]) : 'ยังไม่คอนเฟิร์ม';
    $fuel_cost = isset($variant_post_meta['fuel_cost'][0]) ? 'THB ' . number_format($variant_post_meta['fuel_cost'][0]) : 'ยังไม่คอนเฟิร์ม';

    $book_test_drive_url = home_url('/book-test-drive') . '/?make=' . $make . '&model=' . $model;

    ob_start();
?>
    <div class="container">
        <div class="row">
            <!-- Gallery Section (Left side) -->
            <div class="col-md-5">
                <div class="image-container">
                    <a href="<?php echo $_SERVER['REQUEST_URI'] . '/gallery'; ?>">
                        <img src="<?php echo $image_url; ?>" alt="Variant Image">
                    </a>
                </div>
            </div>

            <!-- Specs Section (Right side) -->
            <div class="col-md-7">
                <div style="margin-top: -10px;">
                    <div class="price-range"><?php echo $retail_price; ?></div>

                    <span class="widget-title"><?php esc_html_e('ราคา รถมอเตอร์ไซค์ ' . $variant_post_title . ' ที่ไทย', 'voiture'); ?></span>

                    <div id="listing-detail-description" class="description inner">
                        <div class="ownership-cost-title-con">
                            <h2 class="ownership-cost-title wa-title-text"><?php esc_html_e($variant_post_title . ' Ownership Cost', 'voiture'); ?></h2>

                        </div>
                        <div class="container-ownership-cost">
                            <div class="costs-container">
                                <div class="cost-item">
                                    <div class="cost-icon road-tax-icon"></div>
                                    <span class="cost-label"><?php esc_html_e('Road Tax Cost*', 'your-textdomain'); ?></span>
                                    <div class="price-con">
                                        <span class="cost-value"><?php echo $road_tax; ?></span>
                                        <span class="cost-period">/year</span>
                                    </div>
                                </div>

                                <div class="cost-item">
                                    <div class="cost-icon insurance-icon"></div>
                                    <span class="cost-label"><?php esc_html_e('Insurance Cost*', 'your-textdomain'); ?></span>
                                    <div class="price-con">
                                        <span class="cost-value"> <?php echo $insurance; ?></span>
                                        <span class="cost-period">/year</span>
                                    </div>
                                </div>

                                <div class="cost-item">
                                    <div class="cost-icon fuel-cost-icon"></div>
                                    <span class="cost-label"><?php esc_html_e('Fuel Cost*', 'your-textdomain'); ?></span>
                                    <div class="price-con">
                                        <span class="cost-value"> <?php echo $fuel_cost; ?></span>
                                        <span class="cost-period">/year</span>
                                    </div>
                                </div>
                            </div>

                            <p class="footnote">* For reference only, you can adjust your real situation with the calculator.</p>
                        </div>
                    </div>


                    <div class="buttons-container-spec">
                        <button class="view-specs-button"><a href="<?php echo $_SERVER['REQUEST_URI'] . 'specs'; ?>">View Specs</a></button>
                        <button class="trade-in-button"><a href="<?php echo $book_test_drive_url; ?>">Book Test Drive</a></button>
                    </div>

                </div>
            </div>
        </div>
    </div>

    <style>
        .ownership-cost-title-con {
            margin-top: 20px;
            margin-bottom: 15px;
        }

        .ownership-cost-title {
            line-height: 32px;
        }

        .price-con {
            margin-top: -7px;
        }

        .cost-item-button {
            width: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
            padding-left: 20px;
            padding-right: 20px;
        }

        .container-ownership-cost {
            display: flow-root;
        }

        .costs-container {
            display: flex;
            align-items: stretch;
            background: #fff;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            border: 1px solid #e0e0e0;
        }

        .cost-item {
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            padding: 20px;
            width: 460px;
            position: relative;
        }

        .cost-item:not(:last-child)::after {
            content: "";
            position: absolute;
            right: 0;
            top: 20px;
            bottom: 20px;
            width: 1px;
            background-color: #e0e0e0;
        }

        .icon {
            width: 24px;
            height: 24px;
            margin-bottom: 4px;
        }

        .cost-label {
            line-height: 18px;
            font-size: 12px;
            color: #8c8c8c;
            margin-top: 7px;
            font-family: "Roboto";
        }

        .cost-value {
            font-size: 16px;
            font-weight: 700;
            color: #262626;
        }

        .cost-period {
            color: #666;
            font-size: 14px;
            font-family: "Roboto";
        }

        .calculator-btn {
            display: flex;
            align-items: center;
            justify-content: center;
            background-color: #fff;
            border: 2px solid #00bcd4;
            border-radius: 4px;
            color: #00bcd4;
            font-size: 16px;
            height: 44px;
            width: 100%;
            cursor: pointer;
            transition: all 0.2s ease;
            font-family: "Roboto";
        }

        .calculator-btn:hover {
            background-color: #f3fcfc;
            color: white;
        }

        .calculator-icon {
            margin-right: 8px;
            width: 20px;
            height: 20px;
        }

        .footnote {
            font-size: 12px;
            color: #888;
            margin-top: 8px;
            margin-bottom: 0px;
            font-family: "Roboto";
        }

        .road-tax-icon {
            width: 24px;
            height: 24px;
            background: url("data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgY2xhc3M9ImRlc2lnbi1pY29uZm9udCI+CiAgPHBhdGggZD0iTTUuMTc0NjM1MzcsMC4wMTI1NDE2NTg2IEM1LjkxNDAwNDExLC0wLjEwNzU0NjE3NSA2LjExNTMxNDExLC0wLjAwMTgxNzEyMjYxIDYuMjUwNTI3MSwwLjEyNTU0NTc4NSBDNi4zMjg0MjUyMiwwLjE5ODkyMTM2MSA2LjM4Mjk2MzcyLDAuMjg1MjgzNTc4IDYuNDA5NTE2NjIsMC4zNzc1MjY2NDIgQzYuNDM1ODI2NDgsMC40Njg5MjU0NDEgNi40MzUxOTUzNSwwLjU2NjQ1ODMwMSA2LjQwMjA0NDM0LDAuNjYyNzg1NjQgTDYuNDAyMDQ0MzQsMC42NjI3ODU2NCBMMS40OTUzMzQ2OSwxNS45MjIwNDU0IEMxLjQ0Nzg4ODAyLDE2LjExNTg0OCAxLjMxOTQ1MDE5LDE2LjI1MDUwNDIgMS4xNTE1MTMyNiwxNi4zMzA4ODIzIEMwLjk3MDYyMjQ4NCwxNi40MTc0NjA0IDAuNzQyNTYxODA2LDE2LjQzOTA5ODUgMC41MzE5NDc2OCwxNi40MDM0OTc4IEMwLjMxNTQwMjE5OCwxNi4zNjY4OTQ1IDAuMTIwMDM1Njc3LDE2LjI2OTYyNDggMC4wMDc3OTk4NTYxNywxNi4xMzA2Mjg0IEMtMC4wNDgyOTM1NDAzLDE2LjA2MTE2MDYgLTAuMDg0ODA3Mzc2NCwxNS45ODEyOTI2IC0wLjA5NjIyMjA1MTEsMTUuODkyNjQxMyBDLTAuMTA2NTE0OTgsMTUuODEyNzAyMSAtMC4wOTY0NTg3OTc2LDE1LjcyNTAwMzggLTAuMDYxMTE2NzE2OSwxNS42MzcwNzQyIEwtMC4wNjExMTY3MTY5LDE1LjYzNzA3NDIgTDQuODQ2ODczNDYsMC4zNzMwODc2OTMgQzUuMTQ2MDEzNDksMC4wMjE1NDc2ODk4IDUuMTUxNTExLDAuMDE4NDQ4MTQ1NSA1LjE1NzE0NTI5LDAuMDE1NDA4NDY3NiBDNS4xNjI5Mzg4NiwwLjAxNDQ1MDExNDQgNS4xNjg3Njg4LDAuMDEzNDk0NTA0IDUuMTc0NjM1MzcsMC4wMTI1NDE2NTg2IFogTTEyLjYxODU0NDYsLTAuMTAwMDM4MzU1IEwxMi42NzQ4ODkyLC0wLjA5OTQ0ODk1MDEgTDE4LjMxMDg0MTYsMTUuNjY0MTE5NyBDMTguMTM2NDk4NSwxNi4yOTI0MDE4IDE3LjkyNDQ1MTIsMTYuMzY2OTE5NCAxNy43MDgwNTI2LDE2LjQwMzQ5NzggQzE3LjQ4MjA1NzUsMTYuNDQxNjk4NCAxNy4yMzU5NzQ3LDE2LjQxMzk5NDcgMTcuMDQ5NjM2LDE2LjMxMDg3MjEgTDE3LjA0OTYzNiwxNi4zMTA4NzIxIEwxMS44MjU5ODk2LDAuNjI2ODczOTI1IEMxMi4xNDM3MzU1LC0wLjA5MzE4Nzc4MTYgMTIuMzkwODY1OSwtMC4xMDQ1MTQ0ODEgMTIuNjc0ODg5MiwtMC4wOTk0NDg5NTAxIFogTTkuMTE4ODY0NTksMTIuMTEyOTQzMiBDOS4zNjA4NjIxNSwxMi4xMTAxOTUyIDkuNTc3NjAzMjgsMTIuMTg4Njc5IDkuNzIzOTcwMDIsMTIuMzEwODYyOSBMOS43MjM5NzAwMiwxMi4zMTA4NjI5IEw5LjkzNDA2MTg3LDE1LjgxMTE0MTEgQzkuNTYyNDkxOTYsMTYuMzU5OTEzMSA5LjM0MzMxMTc1LDE2LjM5OTA5MTkgOS4xMzA5NTg0NCwxNi4zOTkwOTE5IEM4LjkwMDIwMjgxLDE2LjM5OTA5MTkgOC42NzA3MDA1OCwxNi4zMzEwNzY4IDguNTIwMTE2NjYsMTYuMjAxMTMwNCBMOC41MjAxMTY2NiwxNi4yMDExMzA0IEw4LjMyNzk3NDM5LDEyLjcwMDY1NDMgQzguNzAwODU4MTEsMTIuMTY1MDg0NSA4LjkwMjE5MDY4LDEyLjExNTM5NjIgOS4xMTg4NjQ1OSwxMi4xMTI5NDMyIFogTTkuMTE4ODY0NjQsNC45ODg3NjcwNCBDOS4zNjA4NjQ5LDQuOTg2MDE5MSA5LjU3NzYxNTg5LDUuMDY0NTA0ODMgOS43MjM5ODUwNyw1LjE4NjY5MTMyIEw5LjcyMzk4NTA3LDUuMTg2NjkxMzIgTDkuOTM0MDYxNzQsOC42ODY5NjU2NSBDOS41NjI0OTE5LDkuMjM1NzM3NDUgOS4zNDMzMTE3Myw5LjI3NDkxNjI4IDkuMTMwOTU4NDQsOS4yNzQ5MTYyOCBDOC45MDAyMDI4MSw5LjI3NDkxNjI4IDguNjcwNzAwNTgsOS4yMDY5MDExMiA4LjUyMDExNjY2LDkuMDc2OTU0NyBMOC41MjAxMTY2Niw5LjA3Njk1NDcgTDguMzI3OTc0MzksNS41NzY0Nzg1OSBDOC43MDA4NjAzMiw1LjA0MDkwNiA4LjkwMjE5MzA2LDQuOTkxMjE4MjcgOS4xMTg4NjQ2NCw0Ljk4ODc2NzA0IFogTTkuMTE4ODY0NjQsLTAuMDk5OTI5ODY1NyBDOS4zNTg2MTkyOSwtMC4xMDI2NTIzMDkgOS41NzM1OTEwOSwtMC4wMjU2NDE2ODMyIDkuNzE5ODkyMjgsMC4wOTQ2MDQyODI3IEw5LjcxOTg5MjI4LDAuMDk0NjA0MjgyNyBMOS45MzY4NDY4NCwxLjU1ODY3MTMyIEM5LjU2MzU4NjA3LDIuMTExMzY2MTggOS4zNDM4NDE3NSwyLjE1MDc0MDg0IDkuMTMwOTU4NDQsMi4xNTA3NDA4NCBDOC45MDIwOTA5NCwyLjE1MDc0MDg0IDguNjc0NDU2NDMsMi4wODM4MzQxNiA4LjUyMzgyODg4LDEuOTU1OTU2NTYgTDguNTIzODI4ODgsMS45NTU5NTY1NiBMOC4zMjUzNzE1MSwwLjQ5MTUyNDEwOSBDOC42OTk5MjE3NiwtMC4wNDc1NTY1NTI1IDguOTAxNjg2NjgsLTAuMDk3NDcwODExMyA5LjExODg2NDY0LC0wLjA5OTkyOTg2NTcgWiIgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoMi44OCAzLjg0KSIgZmlsbD0iIzQ4NDg0OCIgZmlsbC1ydWxlPSJub256ZXJvIiBzdHJva2U9IiM0ODQ4NDgiIHN0cm9rZS13aWR0aD0iLjIiLz4KPC9zdmc+Cg==") no-repeat center center / cover;
        }

        .insurance-icon {
            width: 24px;
            height: 24px;
            background: url("data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgY2xhc3M9ImRlc2lnbi1pY29uZm9udCI+CiAgPHBhdGggZD0iTTkuODc5Mzc5NTksMS4wODY5NTUxIEw5Ljg3OTM3OTU5LDAuNzU5Mzc5NTkyIEM5Ljg3OTM3OTU5LDAuMzM4NzQyODU3IDkuNTQwNjM2NzMsMCA5LjEyLDAgQzguNjk5MzYzMjcsMCA4LjM2MDYyMDQxLDAuMzM4NzQyODU3IDguMzYwNjIwNDEsMC43NTkzNzk1OTIgTDguMzYwNjIwNDEsMS4wODY5NTUxIEMzLjY4NTIyNDQ5LDEuNDQ0MzEwMiAwLDUuMDY2MjUzMDYgMCw5LjQ2OTkxMDIgQzAsOS44OTA1NDY5NCAwLjMzODc0Mjg1NywxMC4yMjkyODk4IDAuNzU5Mzc5NTkyLDEwLjIyOTI4OTggTDguMzYwNjIwNDEsMTAuMjI5Mjg5OCBMOC4zNjA2MjA0MSwxNS42MjY4NDA4IEM4LjM2MDYyMDQxLDE2LjIyOTg3NzYgNy43OTQ4MDgxNiwxNi43MTc1MTg0IDcuMDk4NzEwMiwxNi43MTc1MTg0IEM2LjQwMjYxMjI0LDE2LjcxNzUxODQgNS44MzY4LDE2LjIyOTg3NzYgNS44MzY4LDE1LjYyNjg0MDggQzUuODM2OCwxNS4yMDYyMDQxIDUuNDk4MDU3MTQsMTQuODY3NDYxMiA1LjA3NzQyMDQxLDE0Ljg2NzQ2MTIgQzQuNjU2NzgzNjcsMTQuODY3NDYxMiA0LjMxODA0MDgyLDE1LjIwNjIwNDEgNC4zMTgwNDA4MiwxNS42MjY4NDA4IEM0LjMxODA0MDgyLDE3LjA2NzQyODYgNS41NjUwNjEyMiwxOC4yMzYyNzc2IDcuMDk4NzEwMiwxOC4yMzYyNzc2IEM4LjYzMjM1OTE4LDE4LjIzNjI3NzYgOS44NzkzNzk1OSwxNy4wNjM3MDYxIDkuODc5Mzc5NTksMTUuNjI2ODQwOCBMOS44NzkzNzk1OSwxMC4yMjkyODk4IEwxNy40ODA2MjA0LDEwLjIyOTI4OTggQzE3LjkwMTI1NzEsMTAuMjI5Mjg5OCAxOC4yNCw5Ljg5MDU0Njk0IDE4LjI0LDkuNDY5OTEwMiBDMTguMjQsNS4wNjYyNTMwNiAxNC41NTQ3NzU1LDEuNDQ0MzEwMiA5Ljg3OTM3OTU5LDEuMDg2OTU1MSBMOS44NzkzNzk1OSwxLjA4Njk1NTEgWiBNNi40NjU4OTM4OCwzLjAxMTQ2MTIyIEM1LjY4NDE3OTU5LDQuMzgxMzIyNDUgNS4xNzQyMDQwOCw2LjM4NCA1LjA4MTE0Mjg2LDguNzEwNTMwNjEgTDEuNTY3MTUxMDIsOC43MTA1MzA2MSBDMS44ODM1NTkxOCw2LjA4OTkyNjUzIDMuODMwNCwzLjkwNDg0ODk4IDYuNDY1ODkzODgsMy4wMTE0NjEyMiBaIE02LjYwMzYyNDQ5LDguNzEwNTMwNjEgQzYuNzYzNjg5OCw0Ljg5MTI5Nzk2IDguMTI2MTA2MTIsMi41Nzk2NTcxNCA5LjEyLDIuNTc5NjU3MTQgQzEwLjExMzg5MzksMi41Nzk2NTcxNCAxMS40NzYzMTAyLDQuODkxMjk3OTYgMTEuNjM2Mzc1NSw4LjcxMDUzMDYxIEw2LjYwMzYyNDQ5LDguNzEwNTMwNjEgWiBNMTMuMTU4ODU3MSw4LjcxMDUzMDYxIEMxMy4wNjU3OTU5LDYuMzg0IDEyLjU1OTU0MjksNC4zODUwNDQ5IDExLjc3NDEwNjEsMy4wMTE0NjEyMiBDMTQuNDA5NiwzLjkwNDg0ODk4IDE2LjM1NjQ0MDgsNi4wODk5MjY1MyAxNi42NzI4NDksOC43MTA1MzA2MSBMMTMuMTU4ODU3MSw4LjcxMDUzMDYxIFoiIHRyYW5zZm9ybT0idHJhbnNsYXRlKDIuODggMi44OCkiIGZpbGw9IiM0ODQ4NDgiIGZpbGwtcnVsZT0ibm9uemVybyIvPgo8L3N2Zz4K") no-repeat center center / cover;
        }

        .fuel-cost-icon {
            width: 24px;
            height: 24px;
            background: url("data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgY2xhc3M9ImRlc2lnbi1pY29uZm9udCI+CiAgPHBhdGggZD0iTTE1LjU0MjkwNjIsMjAuNjkyNSBDMTUuMTk3MzQzOCwyMC42OTI1IDE0LjkxNTkwNjIsMjAuNDAxOTQ4MSAxNC45MTU5MDYyLDIwLjA0MTA3MTQgTDE0LjkxNTkwNjIsNC43NDkxNTU4NCBDMTQuOTE1OTA2Miw0LjQzNjM5NjEgMTQuNjcxODc1LDQuMTgyODU3MTQgMTQuMzcwODQzNyw0LjE4Mjg1NzE0IEw2LjU0NzU5Mzc1LDQuMTgyODU3MTQgQzYuMjQ2NTYyNSw0LjE4Mjg1NzE0IDYuMDAyNTMxMjUsNC40MzYzOTYxIDYuMDAyNTMxMjUsNC43NDkxNTU4NCBMNi4wMDI1MzEyNSwyMC4wNDEwNzE0IEM2LjAwMjUzMTI1LDIwLjQwMTk0ODEgNS43MjEwOTM3NSwyMC42OTI1IDUuMzc1NTMxMjUsMjAuNjkyNSBDNS4wMjk5Njg3NSwyMC42OTI1IDQuNzQ4NTMxMjUsMjAuNDAxOTQ4MSA0Ljc0ODUzMTI1LDIwLjA0MTA3MTQgTDQuNzQ4NTMxMjUsNC43NDkxNTU4NCBDNC43NDg1MzEyNSwzLjcxODM0NDE2IDUuNTU1NDM3NSwyLjg4IDYuNTQ3NTkzNzUsMi44OCBMMTQuMzY5MDYyNSwyLjg4IEMxNS4zNjEyMTg4LDIuODggMTYuMTY4MTMzNCwzLjcxODM0NDE2IDE2LjE2ODEzMzQsNC43NDkxNTU4NCBMMTYuMTY4MTMzNCwyMC4wNDEwNzE0IEMxNi4xNjk5MDYzLDIwLjQwMTk0ODEgMTUuODg4NDY4NywyMC42OTI1IDE1LjU0MjkwNjIsMjAuNjkyNSBaIE0xNy40MDk2NTYzLDIxLjEyIEwzLjUwNywyMS4xMiBDMy4xNjE0Mzc1LDIxLjEyIDIuODgsMjAuODI3NTk3NCAyLjg4LDIwLjQ2ODU3MTQgQzIuODgsMjAuMTA5NTQ1NSAzLjE2MTQzNzUsMTkuODE3MTQyOSAzLjUwNywxOS44MTcxNDI5IEwxNy40MDk2NTYzLDE5LjgxNzE0MjkgQzE3Ljc1NTIxODgsMTkuODE3MTQyOSAxOC4wMzY2NTYzLDIwLjEwNzY5NDggMTguMDM2NjU2MywyMC40Njg1NzE0IEMxOC4wMzY2NTYzLDIwLjgyOTQ0ODEgMTcuNzU3LDIxLjEyIDE3LjQwOTY1NjMsMjEuMTIgWiBNMTMuNTE3NjI1LDcuMTY0MjUzMjUgTDcuMzk5MDMxMjUsNy4xNjQyNTMyNSBDNy4wNTM0Njg3NSw3LjE2NDI1MzI1IDYuNzcyMDMxMjUsNi44NzM3MDEzIDYuNzcyMDMxMjUsNi41MTI4MjQ2OCBDNi43NzIwMzEyNSw2LjE1MTk0ODA1IDcuMDUzNDY4NzUsNS44NjEzOTYxIDcuMzk5MDMxMjUsNS44NjEzOTYxIEwxMy41MTc2MjUsNS44NjEzOTYxIEMxMy44NjMxODc1LDUuODYxMzk2MSAxNC4xNDQ2MjUsNi4xNTE5NDgwNSAxNC4xNDQ2MjUsNi41MTI4MjQ2OCBDMTQuMTQ0NjI1LDYuODczNzAxMyAxMy44NjQ5Njg3LDcuMTY0MjUzMjUgMTMuNTE3NjI1LDcuMTY0MjUzMjUgWiBNMTYuNzY4NDA2MywxNi40NDE1NTg0IEwxNi43Njg0MDYzLDEwLjAzODMxMTcgTDE1LjU2MDcxODgsMTAuMDM4MzExNyBDMTUuMjE1MTU2MywxMC4wMzgzMTE3IDE0LjkzMzcxODgsOS43NDc3NTk3NCAxNC45MzM3MTg4LDkuMzg2ODgzMTIgQzE0LjkzMzcxODgsOS4wMjYwMDY0OSAxNS4yMTUxNTYyLDguNzM1NDU0NTUgMTUuNTYwNzE4OCw4LjczNTQ1NDU1IEwxNy4wNzMsOC43MzU0NTQ1NSBDMTcuNjAwMjUsOC43MzU0NTQ1NSAxNy45NDU4MTI1LDkuMTUgMTguMDEzNSw5LjU2MDg0NDE2IEMxOC4wMTg4NDM4LDkuNTk3ODU3MTQgMTguMDIyNDA2Myw5LjYzNDg3MDEzIDE4LjAyMjQwNjMsOS42NzE4ODMxMiBMMTguMDIyNDA2MywxNi40NDE1NTg0IEMxOC4wMjI0MDYzLDE3LjU1OTM1MDYgMTguNTIyOTM3NSwxNy43OTI1MzI1IDE4Ljk0MzMxMjUsMTcuNzkyNTMyNSBDMTkuMzM4NzUsMTcuNzkyNTMyNSAxOS44NjYsMTcuNjUxODgzMSAxOS44NjYsMTYuNDQzNDA5MSBMMTkuODY2LDcuNTc2OTQ4MDUgTDE4LjY0OTQwNjMsNS41MDA1MTk0OCBDMTguNDY5NSw1LjE5MzMxMTY5IDE4LjU2MzkwNjMsNC43OTE3MjA3OCAxOC44NTk1OTM4LDQuNjA0ODA1MTkgQzE5LjE1NTI4MTMsNC40MTc4ODk2MSAxOS41NDAwMzEzLDQuNTE1OTc0MDMgMTkuNzIxNzE4OCw0LjgyMzE4MTgyIEwyMS4wMjkxNTYzLDcuMDU1MDY0OTQgQzIxLjA4OTcxODgsNy4xNTY4NTA2NSAyMS4xMiw3LjI3MzQ0MTU2IDIxLjEyLDcuMzkzNzMzNzcgTDIxLjEyLDE2LjQ0MTU1ODQgQzIxLjEyLDE4LjM5OTU0NTUgMTkuOTk2MDMxMywxOS4wOTUzODk2IDE4Ljk0MzMxMjUsMTkuMDk1Mzg5NiBDMTcuOTQwNDY4OCwxOS4wOTUzODk2IDE2Ljc2ODQwNjMsMTguMzk5NTQ1NSAxNi43Njg0MDYzLDE2LjQ0MTU1ODQgWiBNMTcuMTAxNzY1NiwxMC4wNzkwNTg0IEwxNy4xMzE3NjU2LDEwLjA3OTA1ODQgTDE3LjEwMTc2NTYsMTAuMDc5MDU4NCBaIE0yMC40MzYsOC4wNzY2MjMzOCBMMTguNDE3ODQzOCw4LjA0MzMxMTY5IEMxOC4wNzIyODEzLDguMDM3NzU5NzQgMTcuNzk2MTg3NSw3Ljc0MTY1NTg0IDE3LjgwMTUzMTMsNy4zODI2Mjk4NyBDMTcuODA2ODc1LDcuMDI3MzA1MTkgMTguMDg2NTMxMyw2Ljc0MjMwNTE5IDE4LjQyODUzMTMsNi43NDIzMDUxOSBMMTguNDM5MjE4OCw2Ljc0MjMwNTE5IEwyMC40NTczNzUsNi43NzU2MTY4OCBDMjAuODAyOTM3NSw2Ljc4MTE2ODgzIDIxLjA3OTAzMTIsNy4wNzcyNzI3MyAyMS4wNzM2ODc1LDcuNDM2Mjk4NyBDMjEuMDY4MzQzOCw3Ljc5MTYyMzM4IDIwLjc4ODY4NzUsOC4wNzY2MjMzOCAyMC40NDY2ODc1LDguMDc2NjIzMzggTDIwLjQzNiw4LjA3NjYyMzM4IFoiIGZpbGw9IiM0ODQ4NDgiIGZpbGwtcnVsZT0ibm9uemVybyIvPgo8L3N2Zz4K") no-repeat center center / cover;
        }
    </style>

    <!-- Slick CSS -->
    <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/slick-carousel@1.8.1/slick/slick.css" />
    <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/slick-carousel@1.8.1/slick/slick-theme.css" />


    <!-- Slick JS -->
    <script type="text/javascript" src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/slick-carousel@1.8.1/slick/slick.min.js"></script>


    <script type="text/javascript">
        $(document).ready(function() {
            // Initialize the Slick slider
            $('.carousel-images').slick({
                infinite: true,
                slidesToShow: 1,
                slidesToScroll: 1,
                autoplay: true,
                autoplaySpeed: 3000,
                arrows: false
            });


            // Custom button functionality
            $('.carousel-button.prev').click(function() {
                $('.carousel-images').slick('slickPrev'); // Go to the previous slide
            });


            $('.carousel-button.next').click(function() {
                $('.carousel-images').slick('slickNext'); // Go to the next slide
            });
            $('.slick-track').css({
                'gap': '0px',
            });
        });
    </script>


<?php
    return ob_get_clean();
}
add_shortcode('motor_variant_overview', 'motor_variant_overview_shortcode');
